package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	SYSLOG_ACTION_OPEN        = 1
	SYSLOG_ACTION_READ        = 2
	SYSLOG_ACTION_READ_ALL    = 3
	SYSLOG_ACTION_SIZE_BUFFER = 10
)

var (
	// see https://en.wikipedia.org/wiki/Syslog#Facility
	FACILITY = []string{
		"kern", "user", "mail", "daemon", "auth", "syslog", "lpr", "news",
		"uucp", "cron", "authpriv", "ftp", "ntp", "security", "console", "solaris-cron",
		"local0", "local1", "local2", "local3", "local4", "local5", "local6", "local7",
	}
	SEVERITY = []string{
		"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug",
	}
)

type klogmessage struct {
	message       string
	facility      string
	severity      string
	seq           int
	labels        []string
	readTimestamp time.Time
}

func messageDecode(data []byte) string {
	hex2byte := func(high byte, low byte) byte {
		// 0..9: byte value 48..57
		// a..f: byte value 97..102
		// A..F: byte calue 65..70
		h := high - 48
		if high >= 65 {
			if high >= 97 {
				h = high - 97 + 10
			} else {
				h = high - 65 + 10
			}
		}
		l := low - 48
		if low >= 65 {
			if low >= 97 {
				l = low - 97 + 10
			} else {
				l = low - 65 + 10
			}
		}
		return (h&0xf)*16 + (l & 0xf)
	}
	ip := 0
	op := 0
	for ip < len(data) {
		if data[ip] != '\\' || ip+1 >= len(data) {
			if ip != op {
				data[op] = data[ip]
			}
			ip++
			op++
			continue
		}
		if ip+1 >= len(data) {
			// broken input, abort
			return string(data[:op])
		}
		if data[ip+1] == '\\' {
			data[op] = '\\'
			op++
			ip += 2
			continue
		}
		if data[ip+1] == 'x' && ip+3 < len(data) {
			data[op] = hex2byte(data[ip+2], data[ip+3])
			op++
			ip += 4
			continue
		}
		// broken input, abort
		return string(data[:op])
	}
	return string(data[:op])
}

func parse(data []byte) (*klogmessage, error) {
	// documentation: https://www.kernel.org/doc/Documentation/ABI/testing/dev-kmsg

	// round one: raw format
	// "fields;message\nlabels"
	var rawFields, message, rawLabels string
	for i := 0; i < len(data); i++ {
		if data[i] != 0x3b {
			continue
		}
		rawFields = string(data[0:i])
		data = data[i+1:]
		break
	}
	for i := 0; i < len(data); i++ {
		if data[i] != 0x0a {
			continue
		}
		message = messageDecode(data[0:i])
		rawLabels = string(data[i+1:])
		break
	}

	result := klogmessage{
		message:       message,
		readTimestamp: time.Now().UTC(),
	}

	// round 2: fields are comma seperated
	// <faciltiy+priority>,<sequence number>,<timestamp>,<flags>
	fields := strings.Split(rawFields, ",")
	if len(fields) < 4 {
		return nil, fmt.Errorf(
			"unhandled set of fields in kernel message "+
				"expected >= 4 fields, got %v\n%s\n%s\n",
			len(fields),
			rawFields,
			string(data))
	}

	facilityPriority, err := strconv.Atoi(fields[0])
	if err != nil {
		return nil, fmt.Errorf("can't parse klog facility prefix in %s: %s", rawFields, err)
	}
	result.severity = SEVERITY[facilityPriority&0x7]
	facility := facilityPriority / 8
	result.facility = "<invalid>"
	if facility >= 0 && facility < len(FACILITY) {
		result.facility = FACILITY[facility]
	}

	result.seq, err = strconv.Atoi(fields[1])
	if err != nil {
		return nil, fmt.Errorf("can't parse klog sequence number in %s: %s", rawFields, err)
	}

	// round 3: exta key=value pairs are added on additional lines, one entry per line
	if len(rawLabels) > 3 {
		extraLabels := strings.Split(strings.Trim(rawLabels, "\n "), "\n")
		for i, entry := range extraLabels {
			extraLabels[i] = strings.Trim(entry, "\n ")
		}
		result.labels = extraLabels
	}

	return &result, nil
}

func klog(c chan *klogmessage) error {
	// check the kernel log buffer size
	buffer_size, err := syscall.Klogctl(SYSLOG_ACTION_SIZE_BUFFER, nil)
	if err != nil {
		return err
	}

	fmt.Printf("kernel log buffer size: %d bytes\n", buffer_size)

	// allocate enough space
	buffer := make([]byte, buffer_size, buffer_size)

	kmsg, err := os.Open("/dev/kmsg")
	if err != nil {
		return err
	}
	for {
		// kmesg will read _exactly_ one message
		bytes_read, err := kmsg.Read(buffer)
		if err != nil {
			return err
		}
		msg, err := parse(buffer[:bytes_read])
		if err != nil {
			return err
		}
		c <- msg
	}
}
