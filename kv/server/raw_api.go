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
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			Error: err.Error(),
		}, err
	}
	b, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			Error: err.Error(),
		}, err
	}
	// 这里NotFound需要单独处理，注释有写，从badge查的时候也要有特殊的错误判断
	var notFound bool
	if b == nil {
		notFound = true
	}
	return &kvrpcpb.RawGetResponse{
		NotFound: notFound,
		Value:    b,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}
	err := server.storage.Write(req.Context, []storage.Modify{{put}})
	var s string
	if err != nil {
		s = err.Error()
	}
	return &kvrpcpb.RawPutResponse{
		Error: s,
	}, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	del := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	err := server.storage.Write(req.Context, []storage.Modify{{del}})
	var s string
	if err != nil {
		s = err.Error()
	}
	return &kvrpcpb.RawDeleteResponse{
		Error: s,
	}, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	defer reader.Close()
	if err != nil {
		return &kvrpcpb.RawScanResponse{
			Error: err.Error(),
		}, err
	}
	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	nums := uint32(0)
	kvPairs := make([]*kvrpcpb.KvPair, 0)
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		if nums >= req.Limit {
			break
		}
		item := iter.Item()
		var v []byte
		// 为什么要用valueCopy，不用value
		v, err = item.ValueCopy(v)
		if err != nil {
			// 这里最好怎么处理？返回错误还是continue
			continue
		}
		kvPairs = append(kvPairs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: v,
		})
		nums++
	}
	return &kvrpcpb.RawScanResponse{
		Kvs: kvPairs,
	}, nil
}
