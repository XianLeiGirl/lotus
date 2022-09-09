// Code generated by github.com/whyrusleeping/cbor-gen. DO NOT EDIT.

package vm

import (
	"fmt"
	"io"
	"math"
	"sort"

	cid "github.com/ipfs/go-cid"
	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"

	types "github.com/xianleigirl/lotus/chain/types"
)

var _ = xerrors.Errorf
var _ = cid.Undef
var _ = math.E
var _ = sort.Sort

var lengthBufFvmExecutionTrace = []byte{132}

func (t *FvmExecutionTrace) MarshalCBOR(w io.Writer) error {
	if t == nil {
		_, err := w.Write(cbg.CborNull)
		return err
	}

	cw := cbg.NewCborWriter(w)

	if _, err := cw.Write(lengthBufFvmExecutionTrace); err != nil {
		return err
	}

	// t.Msg (types.Message) (struct)
	if err := t.Msg.MarshalCBOR(cw); err != nil {
		return err
	}

	// t.MsgRct (types.MessageReceipt) (struct)
	if err := t.MsgRct.MarshalCBOR(cw); err != nil {
		return err
	}

	// t.Error (string) (string)
	if len(t.Error) > cbg.MaxLength {
		return xerrors.Errorf("Value in field t.Error was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajTextString, uint64(len(t.Error))); err != nil {
		return err
	}
	if _, err := io.WriteString(w, string(t.Error)); err != nil {
		return err
	}

	// t.Subcalls ([]vm.FvmExecutionTrace) (slice)
	if len(t.Subcalls) > cbg.MaxLength {
		return xerrors.Errorf("Slice value in field t.Subcalls was too long")
	}

	if err := cw.WriteMajorTypeHeader(cbg.MajArray, uint64(len(t.Subcalls))); err != nil {
		return err
	}
	for _, v := range t.Subcalls {
		if err := v.MarshalCBOR(cw); err != nil {
			return err
		}
	}
	return nil
}

func (t *FvmExecutionTrace) UnmarshalCBOR(r io.Reader) (err error) {
	*t = FvmExecutionTrace{}

	cr := cbg.NewCborReader(r)

	maj, extra, err := cr.ReadHeader()
	if err != nil {
		return err
	}
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()

	if maj != cbg.MajArray {
		return fmt.Errorf("cbor input should be of type array")
	}

	if extra != 4 {
		return fmt.Errorf("cbor input had wrong number of fields")
	}

	// t.Msg (types.Message) (struct)

	{

		b, err := cr.ReadByte()
		if err != nil {
			return err
		}
		if b != cbg.CborNull[0] {
			if err := cr.UnreadByte(); err != nil {
				return err
			}
			t.Msg = new(types.Message)
			if err := t.Msg.UnmarshalCBOR(cr); err != nil {
				return xerrors.Errorf("unmarshaling t.Msg pointer: %w", err)
			}
		}

	}
	// t.MsgRct (types.MessageReceipt) (struct)

	{

		b, err := cr.ReadByte()
		if err != nil {
			return err
		}
		if b != cbg.CborNull[0] {
			if err := cr.UnreadByte(); err != nil {
				return err
			}
			t.MsgRct = new(types.MessageReceipt)
			if err := t.MsgRct.UnmarshalCBOR(cr); err != nil {
				return xerrors.Errorf("unmarshaling t.MsgRct pointer: %w", err)
			}
		}

	}
	// t.Error (string) (string)

	{
		sval, err := cbg.ReadString(cr)
		if err != nil {
			return err
		}

		t.Error = string(sval)
	}
	// t.Subcalls ([]vm.FvmExecutionTrace) (slice)

	maj, extra, err = cr.ReadHeader()
	if err != nil {
		return err
	}

	if extra > cbg.MaxLength {
		return fmt.Errorf("t.Subcalls: array too large (%d)", extra)
	}

	if maj != cbg.MajArray {
		return fmt.Errorf("expected cbor array")
	}

	if extra > 0 {
		t.Subcalls = make([]FvmExecutionTrace, extra)
	}

	for i := 0; i < int(extra); i++ {

		var v FvmExecutionTrace
		if err := v.UnmarshalCBOR(cr); err != nil {
			return err
		}

		t.Subcalls[i] = v
	}

	return nil
}
