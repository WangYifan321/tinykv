package standalone_storage

import (
	"errors"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		return nil
	}
	return &StandAloneStorage{
		db: db,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.db.Close()
	if err != nil {
		return err
	}
	return nil
}

type BadgerReader struct {
	txn *badger.Txn
}

func (r *BadgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	bytes, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	return bytes, err
}

func (r *BadgerReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *BadgerReader) Close() {
	r.txn.Discard()
}
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &BadgerReader{
		txn: s.db.NewTransaction(false),
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// 入参是一个批量操作，遍历每一个元素，写入badger中的同一个txn
	// 这里应该有增删改
	err := s.db.Update(func(txn *badger.Txn) error {
		for _, modify := range batch {
			switch modify.Data.(type) {
			case storage.Put:
				_ = txn.Set(engine_util.KeyWithCF(modify.Cf(), modify.Key()), modify.Value())
			case storage.Delete:
				_ = txn.Delete(engine_util.KeyWithCF(modify.Cf(), modify.Key()))
			}
		}
		return nil
	})
	return err
}
