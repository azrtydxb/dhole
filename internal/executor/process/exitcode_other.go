//go:build !unix

package process

import "os/exec"

func exitCode(err *exec.ExitError) int32 { return narrowExit(err.ExitCode()) }
