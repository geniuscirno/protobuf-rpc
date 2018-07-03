package grpc_http_proxy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"sync"

	"github.com/gorilla/schema"
	"github.com/sirupsen/logrus"
)

type MethodDesc struct {
	MethodName string
	Handler    func(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error)
	HttpMethod string
	HttpPath   string
}

type ServiceDesc struct {
	ServiceName string
	HandlerType interface{}
	Methods     []MethodDesc
}

type ServerInfo struct {
	Server     interface{}
	FullMethod string
}

type service struct {
	server interface{}
	md     map[string]*MethodDesc
	mdata  interface{}
}

type Handler func(ctx context.Context, r *http.Request) (resp interface{}, err error)

type Interceptor func(ctx context.Context, r *http.Request, info *ServerInfo, handler Handler) (resp interface{}, err error)

type ErrorHandler func(http.ResponseWriter, *http.Request, error)

type options struct {
	interceptor  Interceptor
	errorHandler ErrorHandler
}

type Server struct {
	mu   sync.Mutex
	m    map[string]*service
	opts options
}

type ServerOption func(*options)

func WithInterceptor(i Interceptor) ServerOption {
	return func(o *options) {
		if o.interceptor != nil {
			panic("The server interceptor was already set and may not be reset.")
		}
		o.interceptor = i
	}
}

func WithErrorHandler(handler ErrorHandler) ServerOption {
	return func(o *options) {
		o.errorHandler = handler
	}
}

func NewServer(opt ...ServerOption) *Server {
	opts := options{}
	for _, o := range opt {
		o(&opts)
	}
	s := &Server{}
	s.m = make(map[string]*service)
	s.opts = opts
	return s
}

func (s *Server) RegisterService(sd *ServiceDesc, ss interface{}) {
	ht := reflect.TypeOf(sd.HandlerType).Elem()
	st := reflect.TypeOf(ss)
	if !st.Implements(ht) {
		logrus.Fatalf("grpc_http_proxy: Server.RegisterService found the handler of type %v that does not satisfy %v", st, ht)
	}
	s.register(sd, ss)
}

func (s *Server) register(sd *ServiceDesc, ss interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	logrus.Debugf("RegisterService(%q)", sd.ServiceName)

	if _, ok := s.m[sd.ServiceName]; ok {
		logrus.Fatalf("grpc_http_proxy: Server.RegisterService found duplicate service registration for %q", sd.ServiceName)
	}

	srv := &service{
		server: ss,
		md:     make(map[string]*MethodDesc),
	}

	for _, method := range sd.Methods {
		http.HandleFunc(method.HttpPath, s.HttpHandler(method, ss))
	}
	s.m[sd.ServiceName] = srv
}

func (s *Server) HttpHandler(method MethodDesc, ss interface{}) func(http.ResponseWriter, *http.Request) {
	f := func(w http.ResponseWriter, r *http.Request) error {
		if r.Method != method.HttpMethod {
			return errors.New("method not allowed")
		}

		handler := func(ctx context.Context, r *http.Request) (interface{}, error) {
			dec := func(v interface{}) error {
				switch r.Header.Get("Content-Type") {
				case "application/json":
					defer r.Body.Close()
					return json.NewDecoder(r.Body).Decode(v)
				default:
					if err := r.ParseForm(); err != nil {
						return err
					}
					decode := schema.NewDecoder()
					decode.IgnoreUnknownKeys(true)
					switch r.Method {
					case "GET":
						return decode.Decode(v, r.Form)
					case "POST":
						return decode.Decode(v, r.PostForm)
					default:
						return errors.New("http methd not implements")
					}
				}
			}

			reply, err := method.Handler(ss, ctx, dec)
			if err != nil {
				return nil, err
			}
			return reply, nil
		}

		var (
			reply interface{}
			err   error
		)

		if s.opts.interceptor == nil {
			reply, err = handler(context.Background(), r)
		} else {
			info := &ServerInfo{
				Server:     ss,
				FullMethod: method.MethodName,
			}
			reply, err = s.opts.interceptor(context.Background(), r, info, handler)
		}
		if err != nil {
			return err
		}
		return json.NewEncoder(w).Encode(reply)
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if err := f(w, r); err != nil {
			if s.opts.errorHandler != nil {
				s.opts.errorHandler(w, r, err)
			} else {
				http.Error(w, err.Error(), http.StatusOK)
			}
		}
	}
}
