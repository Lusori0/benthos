// Copyright (c) 2019 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/ory/dockertest"
	"github.com/ory/dockertest/docker"
)

func TestKafkaIntegration(t *testing.T) {
	t.Skip("Need to work out how to overcome Kafkas advertised port")
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	// Kafka is an absolute unit and takes forever to kick off.
	pool.MaxWait = time.Second * 60 * 1

	zkResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "wurstmeister/zookeeper",
		Tag:        "latest",
	})
	if err != nil {
		t.Fatalf("Could not start zookeeper resource: %s", err)
	}
	defer func() {
		if err = pool.Purge(zkResource); err != nil {
			t.Logf("Failed to clean up zookeeper docker resource: %v", err)
		}
	}()
	zkResource.Expire(900)

	zkAddr := fmt.Sprintf("%v:2181", zkResource.Container.NetworkSettings.IPAddress)
	kafkaResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "wurstmeister/kafka",
		Tag:          "latest",
		ExposedPorts: []string{"9092"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092": []docker.PortBinding{
				{HostPort: "9092"},
			},
		},
		Env: []string{
			"KAFKA_ADVERTISED_HOST_NAME=localhost",
			fmt.Sprintf("KAFKA_ZOOKEEPER_CONNECT=%v", zkAddr),
		},
	})
	if err != nil {
		t.Fatalf("Could not start kafka resource: %s", err)
	}
	defer func() {
		if err = pool.Purge(kafkaResource); err != nil {
			t.Logf("Failed to clean up kafka docker resource: %v", err)
		}
	}()
	kafkaResource.Expire(900)

	address := "localhost:9092"
	if err = pool.Retry(func() error {
		outConf := writer.NewKafkaConfig()
		outConf.Addresses = []string{address}
		outConf.Topic = "pls_ignore_just_testing_connection"
		tmpOutput, serr := writer.NewKafka(outConf, log.Noop(), metrics.Noop())
		if serr != nil {
			return serr
		}
		defer tmpOutput.CloseAsync()
		if serr = tmpOutput.Connect(); serr != nil {
			return serr
		}
		return tmpOutput.Write(message.New([][]byte{
			[]byte("foo message"),
		}))
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	t.Run("TestKafkaSinglePart", func(te *testing.T) {
		testKafkaSinglePart(address, te)
	})
	/*
		t.Run("TestKafkaResumeDurable", func(te *testing.T) {
			testKafkaResumeDurable(address, te)
		})
		t.Run("TestKafkaMultiplePart", func(te *testing.T) {
			testKafkaMultiplePart(address, te)
		})
		t.Run("TestKafkaDisconnect", func(te *testing.T) {
			testKafkaDisconnect(address, te)
		})
	*/
}

func createKafkaInputOutput(
	inConf reader.KafkaCGConfig, outConf writer.KafkaConfig,
) (mInput reader.Async, mOutput writer.Type, err error) {
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	log := log.Noop()
	if mInput, err = reader.NewKafkaCG(inConf, nil, log, metrics.Noop()); err != nil {
		return
	}
	if err = mInput.Connect(ctx); err != nil {
		return
	}
	if mOutput, err = writer.NewKafka(outConf, log, metrics.Noop()); err != nil {
		return
	}
	if err = mOutput.Connect(); err != nil {
		return
	}
	return
}

func testKafkaSinglePart(address string, t *testing.T) {
	topic := "benthos_test_single"

	inConf := reader.NewKafkaCGConfig()
	inConf.ClientID = "benthos_test_single_client"
	inConf.Addresses = []string{address}
	inConf.Topics = []string{topic}

	outConf := writer.NewKafkaConfig()
	outConf.Addresses = []string{address}
	outConf.Topic = topic

	mInput, mOutput, err := createKafkaInputOutput(inConf, outConf)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		mInput.CloseAsync()
		if cErr := mInput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
		mOutput.CloseAsync()
		if cErr := mOutput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
	}()

	N := 10

	wg := sync.WaitGroup{}
	wg.Add(N)

	ctx, done := context.WithTimeout(context.Background(), time.Second*60)
	defer done()

	testMsgs := map[string]struct{}{}
	for i := 0; i < N; i++ {
		str := fmt.Sprintf("hello world: %v", i)
		testMsgs[str] = struct{}{}
		msg := message.New([][]byte{
			[]byte(str),
		})
		msg.Get(0).Metadata().Set("foo", "bar")
		msg.Get(0).Metadata().Set("root_foo", "bar2")
		if gerr := mOutput.Write(msg); gerr != nil {
			t.Fatal(gerr)
		}
		wg.Done()
	}

	lMsgs := len(testMsgs)
	for lMsgs > 0 {
		var actM types.Message
		var ackFn reader.AsyncAckFn
		actM, ackFn, err = mInput.Read(ctx)
		if err != nil {
			t.Fatal(err)
		} else {
			act := string(actM.Get(0).Get())
			if _, exists := testMsgs[act]; !exists {
				t.Errorf("Unexpected message: %v", act)
			}
			delete(testMsgs, act)
		}
		if err = ackFn(ctx, nil); err != nil {
			t.Error(err)
		}
		lMsgs = len(testMsgs)
	}

	wg.Wait()
}

/*
func testKafkaResumeDurable(url string, t *testing.T) {
	subject := "benthos_test_resume_durable"

	inConf := reader.NewKafkaConfig()
	inConf.ClientID = "benthos_test_durable_client"
	inConf.URLs = []string{url}
	inConf.Subject = subject
	inConf.UnsubOnClose = false

	outConf := writer.NewKafkaConfig()
	outConf.URLs = []string{url}
	outConf.Subject = subject

	mInput, mOutput, err := createKafkaInputOutput(inConf, outConf)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		mInput.CloseAsync()
		if cErr := mInput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
		mOutput.CloseAsync()
		if cErr := mOutput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
	}()

	testMsgs := map[string]struct{}{}
	N := 50
	i := 0
	for ; i < (N / 2); i++ {
		str := fmt.Sprintf("hello world: %v", i)
		testMsgs[str] = struct{}{}
		msg := message.New([][]byte{
			[]byte(str),
		})
		msg.Get(0).Metadata().Set("foo", "bar")
		msg.Get(0).Metadata().Set("root_foo", "bar2")
		if gerr := mOutput.Write(msg); gerr != nil {
			t.Fatal(gerr)
		}
	}

	for len(testMsgs) > 0 {
		var actM types.Message
		actM, err = mInput.Read()
		if err != nil {
			t.Error(err)
		} else {
			act := string(actM.Get(0).Get())
			if _, exists := testMsgs[act]; !exists {
				t.Errorf("Unexpected message: %v", act)
			}
			delete(testMsgs, act)
		}
		if err = mInput.Acknowledge(nil); err != nil {
			t.Error(err)
		}
	}

	mInput.CloseAsync()
	if cErr := mInput.WaitForClose(time.Second); cErr != nil {
		t.Error(cErr)
	}

	for ; i < N; i++ {
		str := fmt.Sprintf("hello world: %v", i+N)
		testMsgs[str] = struct{}{}
		msg := message.New([][]byte{
			[]byte(str),
		})
		msg.Get(0).Metadata().Set("foo", "bar")
		msg.Get(0).Metadata().Set("root_foo", "bar2")
		if gerr := mOutput.Write(msg); gerr != nil {
			t.Fatal(gerr)
		}
	}

	if mInput, err = reader.NewKafka(inConf, log.Noop(), metrics.Noop()); err != nil {
		t.Fatal(err)
	}
	if err = mInput.Connect(); err != nil {
		t.Fatal(err)
	}

	for len(testMsgs) > 1 {
		var actM types.Message
		actM, err = mInput.Read()
		if err != nil {
			t.Error(err)
		} else {
			act := string(actM.Get(0).Get())
			if _, exists := testMsgs[act]; !exists {
				t.Errorf("Unexpected message: %v", act)
			}
			delete(testMsgs, act)
		}
		if err = mInput.Acknowledge(nil); err != nil {
			t.Error(err)
		}
	}
}

func testKafkaMultiplePart(url string, t *testing.T) {
	subject := "benthos_test_multi"

	inConf := reader.NewKafkaConfig()
	inConf.ClientID = "benthos_test_multi_client"
	inConf.URLs = []string{url}
	inConf.Subject = subject

	outConf := writer.NewKafkaConfig()
	outConf.URLs = []string{url}
	outConf.Subject = subject

	mInput, mOutput, err := createKafkaInputOutput(inConf, outConf)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		mInput.CloseAsync()
		if cErr := mInput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
		mOutput.CloseAsync()
		if cErr := mOutput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
	}()

	N := 10

	wg := sync.WaitGroup{}
	wg.Add(N)

	testMsgs := map[string]struct{}{}
	for i := 0; i < N; i++ {
		str1 := fmt.Sprintf("hello world: %v part 1", i)
		str2 := fmt.Sprintf("hello world: %v part 2", i)
		str3 := fmt.Sprintf("hello world: %v part 3", i)
		testMsgs[str1] = struct{}{}
		testMsgs[str2] = struct{}{}
		testMsgs[str3] = struct{}{}
		go func(testStr1, testStr2, testStr3 string) {
			msg := message.New([][]byte{
				[]byte(testStr1),
				[]byte(testStr2),
				[]byte(testStr3),
			})
			msg.Get(0).Metadata().Set("foo", "bar")
			msg.Get(1).Metadata().Set("root_foo", "bar2")
			if gerr := mOutput.Write(msg); gerr != nil {
				t.Fatal(gerr)
			}
			wg.Done()
		}(str1, str2, str3)
	}

	lMsgs := len(testMsgs)
	for lMsgs > 0 {
		var actM types.Message
		actM, err = mInput.Read()
		if err != nil {
			t.Error(err)
		} else {
			act := string(actM.Get(0).Get())
			if _, exists := testMsgs[act]; !exists {
				t.Errorf("Unexpected message: %v", act)
			}
			delete(testMsgs, act)
		}
		if err = mInput.Acknowledge(nil); err != nil {
			t.Error(err)
		}
		lMsgs = len(testMsgs)
	}

	wg.Wait()
}

func testKafkaDisconnect(url string, t *testing.T) {
	subject := "benthos_test_disconnect"

	inConf := reader.NewKafkaConfig()
	inConf.ClientID = "benthos_test_disconn_client"
	inConf.URLs = []string{url}
	inConf.Subject = subject

	outConf := writer.NewKafkaConfig()
	outConf.URLs = []string{url}
	outConf.Subject = subject

	mInput, mOutput, err := createKafkaInputOutput(inConf, outConf)
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		mInput.CloseAsync()
		if cErr := mInput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
		mOutput.CloseAsync()
		if cErr := mOutput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
		wg.Done()
	}()

	if _, err = mInput.Read(); err != types.ErrTypeClosed && err != types.ErrNotConnected {
		t.Errorf("Wrong error: %v != %v", err, types.ErrTypeClosed)
	}

	wg.Wait()
}
*/
