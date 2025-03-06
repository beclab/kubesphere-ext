/*
Copyright 2015 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package remotecommand

import (
	"fmt"
	"io"
	"io/ioutil"
	"k8s.io/klog/v2"
	"net/http"
	"sync"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
)

// streamProtocolV2 implements version 2 of the streaming protocol for attach
// and exec. The original streaming protocol was metav1. As a result, this
// version is referred to as version 2, even though it is the first actual
// numbered version.
type streamProtocolV2 struct {
	StreamOptions

	errorStream  io.Reader
	remoteStdin  io.ReadWriteCloser
	remoteStdout io.Reader
	remoteStderr io.Reader
}

var _ streamProtocolHandler = &streamProtocolV2{}

func newStreamProtocolV2(options StreamOptions) streamProtocolHandler {
	return &streamProtocolV2{
		StreamOptions: options,
	}
}

func (p *streamProtocolV2) createStreams(conn streamCreator) error {
	var err error
	headers := http.Header{}

	// set up error stream
	headers.Set(v1.StreamType, v1.StreamTypeError)
	p.errorStream, err = conn.CreateStream(headers)
	if err != nil {
		return err
	}

	// set up stdin stream
	if p.Stdin != nil {
		headers.Set(v1.StreamType, v1.StreamTypeStdin)
		p.remoteStdin, err = conn.CreateStream(headers)
		if err != nil {
			klog.V(0).Infof("set remoteStdin err=%v", err)
			return err
		}
	}

	// set up stdout stream
	if p.Stdout != nil {
		headers.Set(v1.StreamType, v1.StreamTypeStdout)
		p.remoteStdout, err = conn.CreateStream(headers)
		if err != nil {
			klog.V(0).Infof("set remoteStdout err=%v", err)

			return err
		}
	}

	// set up stderr stream
	if p.Stderr != nil && !p.Tty {
		headers.Set(v1.StreamType, v1.StreamTypeStderr)
		p.remoteStderr, err = conn.CreateStream(headers)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *streamProtocolV2) copyStdin() {
	stop := make(chan struct{})
	defer close(stop)
	if p.Stdin != nil {
		var once sync.Once

		// copy from client's stdin to container's stdin
		go func() {
			defer runtime.HandleCrash()

			// if p.stdin is noninteractive, p.g. `echo abc | kubectl exec -i <pod> -- cat`, make sure
			// we close remoteStdin as soon as the copy from p.stdin to remoteStdin finishes. Otherwise
			// the executed command will remain running.
			defer once.Do(func() {
				p.remoteStdin.Close()
				klog.V(0).Infof("remoteStdin close in copyStdin1....")

			})
			klog.V(0).Infof("start.. copy copy from client stdin to container stdin")
			if _, err := io.Copy(p.remoteStdin, readerWrapper{p.Stdin}); err != nil {
				klog.V(0).Infof("copy from client stdin to container stdin. err=%v", err)
				runtime.HandleError(err)
			}
		}()

		// read from remoteStdin until the stream is closed. this is essential to
		// be able to exit interactive sessions cleanly and not leak goroutines or
		// hang the client's terminal.
		//
		// TODO we aren't using go-dockerclient any more; revisit this to determine if it's still
		// required by engine-api.
		//
		// go-dockerclient's current hijack implementation
		// (https://github.com/fsouza/go-dockerclient/blob/89f3d56d93788dfe85f864a44f85d9738fca0670/client.go#L564)
		// waits for all three streams (stdin/stdout/stderr) to finish copying
		// before returning. When hijack finishes copying stdout/stderr, it calls
		// Close() on its side of remoteStdin, which allows this copy to complete.
		// When that happens, we must Close() on our side of remoteStdin, to
		// allow the copy in hijack to complete, and hijack to return.
		go func() {
			defer runtime.HandleCrash()
			defer once.Do(func() {

				klog.V(0).Infof("remoteStdin close in copyStdin2....")

				p.remoteStdin.Close()
			})

			// this "copy" doesn't actually read anything - it's just here to wait for
			// the server to close remoteStdin.
			klog.V(0).Infof("start.. copy copy from remote stdin to discard")

			if _, err := io.Copy(ioutil.Discard, p.remoteStdin); err != nil {
				runtime.HandleError(err)
			}
		}()
	}
}

func (p *streamProtocolV2) copyStdout(wg *sync.WaitGroup) {
	if p.Stdout == nil {
		return
	}

	wg.Add(1)
	go func() {
		defer runtime.HandleCrash()
		defer func() {
			klog.V(0).Infof("copyStdout done..")
		}()
		defer wg.Done()
		// make sure, packet in queue can be consumed.
		// block in queue may lead to deadlock in conn.server
		// issue: https://github.com/kubernetes/kubernetes/issues/96339
		defer io.Copy(ioutil.Discard, p.remoteStdout)

		if _, err := io.Copy(p.Stdout, p.remoteStdout); err != nil {
			klog.V(0).Infof("copy in stdout, err=%v", err)
			runtime.HandleError(err)
		}
	}()
}

func (p *streamProtocolV2) copyStderr(wg *sync.WaitGroup) {
	if p.Stderr == nil || p.Tty {
		return
	}

	wg.Add(1)
	go func() {
		defer runtime.HandleCrash()
		defer func() {
			klog.V(0).Infof("copyStderr done..")
		}()
		defer wg.Done()
		defer io.Copy(ioutil.Discard, p.remoteStderr)

		if _, err := io.Copy(p.Stderr, p.remoteStderr); err != nil {
			klog.V(0).Infof("copy in stderr, err=%v", err)
			runtime.HandleError(err)
		}
	}()
}

func (p *streamProtocolV2) stream(conn streamCreator) error {
	klog.V(0).Infof("stream in v2....")

	if err := p.createStreams(conn); err != nil {
		klog.V(0).Infof("create stream err1=%v", err)
		return err
	}

	klog.V(0).Infof("create stream 1111")

	// now that all the streams have been created, proceed with reading & copying

	errorChan := watchErrorStream(p.errorStream, &errorDecoderV2{})

	klog.V(0).Infof("create stream 2222")

	p.copyStdin()
	klog.V(0).Infof("create stream 3333")

	var wg sync.WaitGroup
	p.copyStdout(&wg)
	klog.V(0).Infof("create stream 4444")

	p.copyStderr(&wg)
	klog.V(0).Infof("create stream 5555")

	// we're waiting for stdout/stderr to finish copying
	klog.V(0).Infof("waiting stream .........")
	wg.Wait()

	// waits for errorStream to finish reading with an error or nil
	a := <-errorChan
	klog.V(0).Infof("stream in v2, a=%v", a)
	return a
}

// errorDecoderV2 interprets the error channel data as plain text.
type errorDecoderV2 struct{}

func (d *errorDecoderV2) decode(message []byte) error {
	return fmt.Errorf("error executing remote command: %s", message)
}
