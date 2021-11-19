package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	val, err := reader.GetCF(req.Cf, req.Key)
	rsp := &kvrpcpb.RawGetResponse{
		Value: val,
	}
	if err != nil {
		rsp.Error = err.Error()
	}
	if len(val) == 0 {
		rsp.NotFound = true
	}
	return rsp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key: req.GetKey(),
		Value: req.GetValue(),
		Cf: req.GetCf(),
	}
	m := []storage.Modify{{put}}
	err := server.storage.Write(nil, m)
	resp := &kvrpcpb.RawPutResponse{}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	del := storage.Put{
		Key: req.GetKey(),
		Cf: req.GetCf(),
	}
	m := []storage.Modify{{del}}
	err := server.storage.Write(nil, m)
	resp := &kvrpcpb.RawDeleteResponse{}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	limit := req.Limit

	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	dbIter := reader.IterCF(req.GetCf())
	defer dbIter.Close()

	var kvs []*kvrpcpb.KvPair
	for dbIter.Seek(req.GetStartKey()); dbIter.Valid(); dbIter.Next() {
		if limit == 0 {
			break
		}
		item := dbIter.Item()
		value, err := item.Value()
		if err != nil {
			continue
		}
		limit = limit - 1
		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: value,
		})
	}
	return &kvrpcpb.RawScanResponse{
		Kvs: kvs,
	}, nil
}
