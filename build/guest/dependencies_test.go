package guestbuild_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestExpiredAPKBudgetDoesNotStartDownload(t *testing.T) {
	script, err := os.ReadFile("dependencies.sh")
	if err != nil {
		t.Fatal(err)
	}
	text := string(script)
	start := strings.Index(text, "apk_download_budget() {")
	end := strings.Index(text[start:], "apk_repositories=") + start
	functions := text[start:end]
	for _, call := range []string{"fetch_apk origin target", "apk_budget=$(apk_download_budget) || exit\nfetch_current origin target \"$apk_budget\""} {
		command := exec.Command("sh")
		command.Stdin = strings.NewReader("set -e\ndate() { echo 2; }\ntimeout() { echo DOWNLOAD_STARTED; exit 99; }\nfetch_current() { echo DOWNLOAD_STARTED; exit 99; }\napk_deadline=1\n" + functions + "\n" + call + "\n")
		output, err := command.CombinedOutput()
		if err == nil || !strings.Contains(string(output), "APK download deadline exceeded") || strings.Contains(string(output), "DOWNLOAD_STARTED") {
			t.Fatalf("expired APK download: output=%s err=%v", output, err)
		}
	}
}
