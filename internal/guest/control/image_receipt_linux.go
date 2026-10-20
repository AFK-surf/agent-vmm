//go:build linux

package control

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"unicode/utf8"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	coreimages "github.com/containerd/containerd/v2/core/images"
	"google.golang.org/protobuf/proto"
)

type imageReceiptLabel struct {
	SourceImage string `json:"source_image,omitempty"`
	Digest      string `json:"digest,omitempty"`
	WriterID    string `json:"writer_id,omitempty"`
	Receipt     string `json:"receipt"`
}

func imageOperationReceipt(record coreimages.Image, kind string) *vmmdv1.ImageOperationReceipt {
	var label imageReceiptLabel
	if json.Unmarshal([]byte(record.Labels[labelPrefix+kind+"-receipt"]), &label) != nil || label.Digest != record.Target.Digest.String() {
		return nil
	}
	bytes, err := base64.StdEncoding.DecodeString(label.Receipt)
	if err != nil {
		return nil
	}
	receipt := new(vmmdv1.ImageOperationReceipt)
	if proto.Unmarshal(bytes, receipt) != nil {
		return nil
	}
	return receipt
}

func setImageOperationReceipt(record *coreimages.Image, kind string, receipt *vmmdv1.ImageOperationReceipt) error {
	if receipt == nil {
		return errors.New("image operation receipt is missing")
	}
	key := labelPrefix + kind + "-receipt"
	label, err := encodeImageReceiptLabel(key, receipt, imageReceiptLabel{Digest: record.Target.Digest.String()})
	if err != nil {
		return err
	}
	if record.Labels == nil {
		record.Labels = make(map[string]string)
	}
	record.Labels[key] = label
	return nil
}

func (r *Runtime) recordImageBuildReceipt(ctx context.Context, namespace, reference, expectedDigest, scope string, receipt *vmmdv1.ImageOperationReceipt) error {
	ctx = namespaceContext(ctx, namespace)
	record, err := r.client.ImageService().Get(ctx, reference)
	if err != nil {
		return err
	}
	if record.Target.Digest.String() != expectedDigest {
		return errors.New("build output changed before receipt publication")
	}
	if err := setImageOperationReceipt(&record, "build", receipt); err != nil {
		return err
	}
	record.Labels[builtControllerLabel] = scope
	_, err = r.client.ImageService().Update(ctx, record, "labels")
	return err
}

func encodeImageReceiptLabel(key string, receipt *vmmdv1.ImageOperationReceipt, label imageReceiptLabel) (string, error) {
	stored := proto.Clone(receipt).(*vmmdv1.ImageOperationReceipt)
	encode := func() (string, error) {
		bytes, err := proto.Marshal(stored)
		if err != nil {
			return "", err
		}
		label.Receipt = base64.StdEncoding.EncodeToString(bytes)
		encoded, err := json.Marshal(label)
		return string(encoded), err
	}
	encoded, err := encode()
	if err != nil || len(key)+len(encoded) <= 4096 {
		return encoded, err
	}
	message := stored.GetStatus().GetMessage()
	low, high := 0, len(message)
	for low < high {
		middle := (low + high + 1) / 2
		start := len(message) - middle
		for start < len(message) && !utf8.RuneStart(message[start]) {
			start++
		}
		if stored.Status != nil {
			stored.Status.Message = message[start:]
		}
		candidate, err := encode()
		if err != nil {
			return "", err
		}
		if len(key)+len(candidate) <= 4096 {
			low = middle
		} else {
			high = middle - 1
		}
	}
	start := len(message) - low
	for start < len(message) && !utf8.RuneStart(message[start]) {
		start++
	}
	if stored.Status != nil {
		stored.Status.Message = message[start:]
	}
	encoded, err = encode()
	if err == nil && len(key)+len(encoded) > 4096 {
		return "", errors.New("image receipt identity and status details exceed containerd label capacity")
	}
	return encoded, err
}
