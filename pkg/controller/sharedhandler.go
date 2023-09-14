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

func (h *SharedHandler) Register(ctx context.Context, name string, handler SharedControllerHandler) {
	h.lock.Lock()
	defer h.lock.Unlock()

	id := atomic.AddInt64(&h.idCounter, 1)
	h.handlers = append(h.handlers, handlerEntry{
		id:      id,
		name:    name,
		handler: handler,
	})

	go func() {
		<-ctx.Done()

		h.lock.Lock()
		defer h.lock.Unlock()

		for i := range h.handlers {
			if h.handlers[i].id == id {
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

	onChangeStart := time.Now()
	// 打印 obj, 记录开始时间
	fmt.Printf("key: %s, obj: %v, time: %v\n", key, obj, onChangeStart)

	for _, handler := range handlers {
		var hasError bool
		reconcileStartTS := time.Now()

		newObj, err := handler.handler.OnChange(key, obj)

		// 打印 handler name , obj
		fmt.Printf("handler name: %s, obj: %v\n", handler.name, obj)

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

	// 打印 obj, 记录结束时间, 耗时
	fmt.Printf("key: %s, obj: %v, time: %v, cost: %v\n", key, obj, time.Now(), time.Since(onChangeStart))

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
