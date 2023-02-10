/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package redis

import (
	"context"
	"fmt"
	"github.com/chaosblade-io/chaosblade-exec-os/exec/category"
	"github.com/chaosblade-io/chaosblade-spec-go/log"
	"github.com/chaosblade-io/chaosblade-spec-go/spec"
	"github.com/go-redis/redis/v8"
)

const (
	SentinelStopBin = "chaos_sentinelStop"
	STATUSOK        = "OK"
	OPTIONNX        = "NX"
	OPTIONXX        = "XX"
	OPTIONGT        = "GT"
	OPTIONLT        = "LT"
)

type SentinelStopActionCommandSpec struct {
	spec.BaseExpActionCommandSpec
}

func NewSentinelStopActionSpec() spec.ExpActionCommandSpec {
	return &SentinelStopActionCommandSpec{
		spec.BaseExpActionCommandSpec{
			ActionMatchers: []spec.ExpFlagSpec{},
			ActionFlags: []spec.ExpFlagSpec{
				&spec.ExpFlag{
					Name: "addr",
					Desc: "The address of redis server",
				},
				&spec.ExpFlag{
					Name: "password",
					Desc: "The password of server",
				},
				&spec.ExpFlag{
					Name: "conf",
					Desc: "The config path of Redis server",
				},
				&spec.ExpFlag{
					Name: "flush-config",
					Desc: "Force Sentinel to rewrite its configuration on disk",
				},
				&spec.ExpFlag{
					Name: "redis-path",
					Desc: "The path of the redis-server command",
				},
			},
			ActionExecutor: &SentinelStopExecutor{},
			ActionExample: `
# Stop local sentinel: 127.0.0.1:26379
./blade create redis sentinel-stop --addr 127.0.0.1:26379 --conf /home/redis-test/sentinel-26379.conf

# Stop remote sentinel: 192.168.56.102:26379
./blade create redis sentinel-stop --addr 192.168.56.102:26379
 --conf /home/redis-test/sentinel-26379.conf --channel ssh --ssh-host 192.168.56.102  --ssh-user root  --install-path /root/chaosblade-1.7.1
`,
			ActionPrograms:   []string{SentinelStopBin},
			ActionCategories: []string{category.SystemTime},
		},
	}
}

func (*SentinelStopActionCommandSpec) Name() string {
	return "sentinel-stop"
}

func (*SentinelStopActionCommandSpec) Aliases() []string {
	return []string{"ss"}
}

func (*SentinelStopActionCommandSpec) ShortDesc() string {
	return "Sentinel Stop"
}

func (k *SentinelStopActionCommandSpec) LongDesc() string {
	if k.ActionLongDesc != "" {
		return k.ActionLongDesc
	}
	return "Stop sentinel"
}

func (*SentinelStopActionCommandSpec) Categories() []string {
	return []string{category.SystemProcess}
}

type SentinelStopExecutor struct {
	channel spec.Channel
}

func (sse *SentinelStopExecutor) Name() string {
	return "sentinel-stop"
}

func (sse *SentinelStopExecutor) Exec(uid string, ctx context.Context, model *spec.ExpModel) *spec.Response {

	addrStr := model.ActionFlags["addr"]
	passwordStr := model.ActionFlags["password"]
	flushConfigStr := model.ActionFlags["flush-config"]
	redisPathStr := model.ActionFlags["redis-path"]
	confStr := model.ActionFlags["conf"]

	if addrStr == "" {
		log.Errorf(ctx, "addr is nil")
		return spec.ResponseFailWithFlags(spec.ParameterLess, "addr")
	}

	cli := redis.NewClient(&redis.Options{
		Addr:     addrStr,
		Password: passwordStr,
	})
	_, err := cli.Ping(cli.Context()).Result()
	if err != nil {
		errMsg := "redis ping error: " + err.Error()
		log.Errorf(ctx, errMsg)
		return spec.ResponseFailWithFlags(spec.ActionNotSupport, errMsg)
	}

	if flushConfigStr != "" {
		// Because redis.Client doesn't have the func `FlushConfig()`, a redis.SentinelClient has to be created
		sentinelCli := redis.NewSentinelClient(&redis.Options{
			Addr: addrStr,
		})
		result, err := sentinelCli.FlushConfig(sentinelCli.Context()).Result()
		if err != nil {
			errMsg := "sentinel flush config error: " + err.Error()
			log.Errorf(ctx, errMsg)
			return spec.ResponseFailWithFlags(spec.ActionNotSupport, errMsg)
		}
		if result != STATUSOK {
			errMsg := fmt.Sprintf("sentinel flush config error: redis command status is %s", result)
			log.Errorf(ctx, errMsg)
			return spec.ResponseFailWithFlags(spec.ActionNotSupport, errMsg)
		}
	}

	if _, ok := spec.IsDestroy(ctx); ok {
		return sse.stop(ctx, redisPathStr, confStr)
	}

	return sse.start(ctx, cli)
}

func (sse *SentinelStopExecutor) SetChannel(channel spec.Channel) {
	sse.channel = channel
}

func (sse *SentinelStopExecutor) stop(ctx context.Context, redisPathStr string, confStr string) *spec.Response {
	var redisPath string
	if redisPathStr != "" {
		redisPath = redisPathStr + "/redis-server"
	} else {
		redisPath = "redis-server"
	}
	return sse.channel.Run(ctx, redisPath, fmt.Sprintf(`"%s" --sentinel`, confStr))
}

func (sse *SentinelStopExecutor) start(ctx context.Context, cli *redis.Client) *spec.Response {
	// If cli.Shutdown() runs successfully, the result will be nil and the error will be "connection refused"
	result, _ := cli.Shutdown(cli.Context()).Result()
	if result != "" {
		errMsg := "redis shutdown error: " + result
		log.Errorf(ctx, errMsg)
		return spec.ResponseFailWithFlags(spec.ActionNotSupport, errMsg)
	}
	return nil
}
