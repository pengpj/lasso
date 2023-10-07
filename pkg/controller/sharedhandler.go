package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rancher/lasso/pkg/metrics"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	ErrIgnore = errors.New("ignore handler error")
)

type handlerEntry struct {
	id      int64
	name    string
	handler SharedControllerHandler
}

type SharedHandler struct {
	// keep first because arm32 needs atomic.AddInt64 target to be mem aligned
	idCounter     int64
	controllerGVR string

	lock     sync.RWMutex
	handlers []handlerEntry
}

// 实现了一个注册函数 Register，通过将处理函数添加到 handlers 切片中，并在上下文完成时从 handlers 中移除相应的处理函数。这样可以动态地注册和取消注册处理函数，并保证并发安全。
func (h *SharedHandler) Register(ctx context.Context, name string, handler SharedControllerHandler) {
	h.lock.Lock()
	defer h.lock.Unlock()

	id := atomic.AddInt64(&h.idCounter, 1)
	h.handlers = append(h.handlers, handlerEntry{
		id:      id,
		name:    name,
		handler: handler,
	})

	// 打印当前 ctx name handlers 列表
	fmt.Printf("register ctx name: %s, controller name : %v, handler name: %v, handlers: %v\n", ctx, h.controllerGVR, name, h.handlers)

	go func() {
		<-ctx.Done()

		h.lock.Lock()
		defer h.lock.Unlock()

		for i := range h.handlers {
			if h.handlers[i].id == id {
				// fmt 打印 ctx, handler name , handlers 列表
				fmt.Printf("ctx done, ctx name: %s, controller name : %v, handler name: %v, handlers: %v\n", ctx, h.controllerGVR, name, h.handlers)
				h.handlers = append(h.handlers[:i], h.handlers[i+1:]...)
				break
			}
		}
	}()
}

func (h *SharedHandler) OnChange(key string, obj runtime.Object) error {
	var (
		errs errorList
	)
	h.lock.RLock()
	handlers := h.handlers
	h.lock.RUnlock()

	start := time.Now()

	for _, handler := range handlers {
		var hasError bool
		reconcileStartTS := time.Now()

		newObj, err := handler.handler.OnChange(key, obj)

		// 打印 key, handler name 处理都很快； 不再打印此处理日志
		// fmt.Printf("traceId %v key: %s, handler name: %s\n", traceId, key, handler.name)

		if err != nil && !errors.Is(err, ErrIgnore) {
			errs = append(errs, &handlerError{
				HandlerName: handler.name,
				Err:         err,
			})
			hasError = true
		}
		metrics.IncTotalHandlerExecutions(h.controllerGVR, handler.name, hasError)
		reconcileTime := time.Since(reconcileStartTS)
		metrics.ReportReconcileTime(h.controllerGVR, handler.name, hasError, reconcileTime.Seconds())

		if newObj != nil && !reflect.ValueOf(newObj).IsNil() {
			meta, err := meta.Accessor(newObj)
			if err == nil && meta.GetUID() != "" {
				// avoid using an empty object
				obj = newObj
			} else if err != nil {
				// assign if we can't determine metadata
				obj = newObj
			}
		}
	}

	// 如果 gvk == version management.cattle.io/v3, kind Cluster ，打印耗时
	if h.controllerGVR == "management.cattle.io/v3, Resource=clusters" {
		fmt.Printf("key: %s, controller name: %s, total time: %v ms\n", key, h.controllerGVR, time.Since(start).Milliseconds())
	}

	return errs.ToErr()
}

type errorList []error

func (e errorList) Error() string {
	buf := strings.Builder{}
	for _, err := range e {
		if buf.Len() > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(err.Error())
	}
	return buf.String()
}

func (e errorList) ToErr() error {
	switch len(e) {
	case 0:
		return nil
	case 1:
		return e[0]
	default:
		return e
	}
}

func (e errorList) Cause() error {
	if len(e) > 0 {
		return e[0]
	}
	return nil
}

type handlerError struct {
	HandlerName string
	Err         error
}

func (h handlerError) Error() string {
	return fmt.Sprintf("handler %s: %v", h.HandlerName, h.Err)
}

func (h handlerError) Cause() error {
	return h.Err
}
