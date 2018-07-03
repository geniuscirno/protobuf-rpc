package message

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
	"sync"

	"github.com/golang/protobuf/proto"
)

const (
	HeaderSize  = 4
	BodyMaxSize = 4096
)

type MethodDesc struct {
	MethodName string
	MethodId   uint32
	Handler    func(interface{}, context.Context, func(interface{}) error) (interface{}, error)
}

type ServiceDesc struct {
	ServiceName string
	HandlerType interface{}
	Methods     []MethodDesc
}

type service struct {
	server interface{}
	md     map[uint32]*MethodDesc
	conn   map[uint32]net.Conn
}

type Server struct {
	mu sync.Mutex

	m *service
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) RegisterService(sd *ServiceDesc, ss interface{}) {
	ht := reflect.TypeOf(sd.HandlerType).Elem()
	st := reflect.TypeOf(ss)

	if !st.Implements(ht) {
		log.Fatalf("rpc: Server.RegisterService found the handler of type %v that does not satisfy %v", st, ht)
	}
	s.register(sd, ss)
}

func (s *Server) register(sd *ServiceDesc, ss interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("rpc: RegisterService(%q)", sd.ServiceName)

	srv := &service{
		server: ss,
		md:     make(map[uint32]*MethodDesc),
		conn:   make(map[uint32]net.Conn),
	}

	for i := range sd.Methods {
		d := &sd.Methods[i]
		srv.md[d.MethodId] = d
	}
	s.m = srv
}

func (s *Server) Serve(lis net.Listener) error {
	for {
		conn, err := lis.Accept()
		if err != nil {
			return err
		}
		go s.ServeConn(conn)
	}
}

type contextConnKey struct{}

func newContextWithSession(sess *Session) context.Context {
	return context.WithValue(context.Background(), contextConnKey{}, sess)
}

func SessionFromContext(ctx context.Context) (*Session, bool) {
	s, ok := ctx.Value(contextConnKey{}).(*Session)
	return s, ok
}

type Session struct {
	Id           uint64
	InputFilter  []func([]byte) []byte
	OutputFilter []func([]byte) []byte
	conn         net.Conn
}

func toHex(b []byte) string {
	s := "["
	for i, v := range b {
		if i < len(b)-1 {
			s += fmt.Sprintf("0x%.2x, ", v)
		} else {
			s += fmt.Sprintf("0x%.2x", v)
		}
	}
	s += "]"
	return s
}

func (sess *Session) AddInputFilter(f func([]byte) []byte) {
	sess.InputFilter = append(sess.InputFilter, f)
}

func (sess *Session) AddOutputFilter(f func([]byte) []byte) {
	sess.OutputFilter = append(sess.OutputFilter, f)
}

func (sess *Session) Recv() (uint32, []byte, error) {
	header := make([]byte, HeaderSize, HeaderSize)
	buf := make([]byte, BodyMaxSize, BodyMaxSize)
	if _, err := io.ReadFull(sess.conn, header); err != nil {
		return 0, nil, err
	}

	size := binary.BigEndian.Uint32(header)

	//id 1 bytes
	//msg size bytes
	if _, err := io.ReadFull(sess.conn, buf[:size]); err != nil {
		return 0, nil, err
	}
	for _, filter := range sess.InputFilter {
		log.Println("rpc:Recv before filter", toHex(buf[:size]))
		buf = filter(buf[:size])
		log.Println("rpc:Recv after filter", toHex(buf[:size]))
	}

	return uint32(buf[0]), buf[1:size], nil
}

func (sess *Session) Send(id uint32, v interface{}) error {
	b, err := proto.Marshal(v.(proto.Message))
	if err != nil {
		return err
	}
	return sess.rawSend(id, b)
}

func (sess *Session) rawSend(id uint32, data []byte) error {
	log.Println("rpc:Send", id, toHex(data))
	data_buffer := bytes.NewBuffer([]byte{})
	binary.Write(data_buffer, binary.BigEndian, uint8(id))
	binary.Write(data_buffer, binary.BigEndian, data)

	payload := data_buffer.Bytes()
	for _, filter := range sess.OutputFilter {
		payload = filter(payload)
	}

	buffer := bytes.NewBuffer([]byte{})
	binary.Write(buffer, binary.BigEndian, uint32(len(payload)))
	binary.Write(buffer, binary.BigEndian, payload)
	_, err := sess.conn.Write(buffer.Bytes())
	return err
}

func (s *Server) ServeConn(conn net.Conn) {
	sess := &Session{conn: conn}
	ctx := newContextWithSession(sess)
	defer func() {
		if e := recover(); e != nil {
			log.Println(e)
		}
	}()

	for {
		id, data, err := sess.Recv()
		log.Println("rpc:Recv", id, toHex(data))
		if err != nil {
			panic(err)
		}

		s.mu.Lock()
		md, ok := s.m.md[id]
		s.mu.Unlock()
		if !ok {
			panic(fmt.Errorf("network.api.id %d not found", id))
		}

		if err := s.call(ctx, md, data); err != nil {
			//panic(err)
			log.Println(sess.Id, err)
		}
		//if err := sess.Send(id, b); err != nil {
		//	panic(err)
		//}
	}
}

func (s *Server) call(ctx context.Context, m *MethodDesc, data []byte) error {
	log.Println("rpc:call", m.MethodId, m.MethodName, toHex(data))

	_, err := m.Handler(s.m.server, ctx, func(v interface{}) error {
		return proto.Unmarshal(data, v.(proto.Message))
	})

	//log.Println("rpc:call reply", m.MethodId, m.MethodName, out)

	if err != nil {
		return err
	}
	return nil
}
