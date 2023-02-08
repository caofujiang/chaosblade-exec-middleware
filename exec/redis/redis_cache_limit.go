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
	"math"
	"strconv"
)

const CacheLimitBin = "chaos_cacheLimit"

type CacheLimitActionCommandSpec struct {
	spec.BaseExpActionCommandSpec
}

func NewCacheLimitActionSpec() spec.ExpActionCommandSpec {
	return &CacheLimitActionCommandSpec{
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
					Name: "size",
					Desc: "The size of cache",
				},
				&spec.ExpFlag{
					Name: "percent",
					Desc: "The percentage of maxmemory",
				},
			},
			ActionExecutor: &CacheLimitExecutor{},
			ActionExample: `
# set maxmemory
blade create redis cache-limit --addr 192.168.56.101:6379 --password 123456  --size 256M
`,
			ActionPrograms:   []string{CacheLimitBin},
			ActionCategories: []string{category.SystemTime},
		},
	}
}

func (*CacheLimitActionCommandSpec) Name() string {
	return "cache-limit"
}

func (*CacheLimitActionCommandSpec) Aliases() []string {
	return []string{"cl"}
}

func (*CacheLimitActionCommandSpec) ShortDesc() string {
	return "Cache Limit"
}

func (k *CacheLimitActionCommandSpec) LongDesc() string {
	if k.ActionLongDesc != "" {
		return k.ActionLongDesc
	}
	return "Set maxmemory of Redis"
}

func (*CacheLimitActionCommandSpec) Categories() []string {
	return []string{category.SystemProcess}
}

type CacheLimitExecutor struct {
	channel spec.Channel
}

func (cle *CacheLimitExecutor) Name() string {
	return "cache-limit"
}

func (cle *CacheLimitExecutor) Exec(uid string, ctx context.Context, model *spec.ExpModel) *spec.Response {
	addrStr := model.ActionFlags["addr"]
	passwordStr := model.ActionFlags["password"]
	sizeStr := model.ActionFlags["size"]
	percentStr := model.ActionFlags["percent"]

	cli := redis.NewClient(&redis.Options{
		Addr:     addrStr,
		Password: passwordStr,
	})
	_, err := cli.Ping(cli.Context()).Result()
	if err != nil {
		log.Errorf(ctx, err.Error())
		return spec.ResponseFailWithFlags(spec.ActionNotSupport, "redis ping error: "+err.Error())
	}

	// `maxmemory` is an interface listwith content similar to `[maxmemory 1024]`
	maxmemory, err := cli.ConfigGet(cli.Context(), "maxmemory").Result()
	if err != nil {
		log.Errorf(ctx, err.Error())
		return spec.ResponseFailWithFlags(spec.ActionNotSupport, "redis get max memory error: "+err.Error())
	}
	// Get the value of maxmemory
	originCacheSize := fmt.Sprint(maxmemory[1])

	if _, ok := spec.IsDestroy(ctx); ok {
		return cle.stop(ctx, cli, originCacheSize)
	}

	return cle.start(ctx, cli, percentStr, originCacheSize, sizeStr)
}

func (cle *CacheLimitExecutor) SetChannel(channel spec.Channel) {
	cle.channel = channel
}

func (cle *CacheLimitExecutor) stop(ctx context.Context, cli *redis.Client, originCacheSize string) *spec.Response {
	result, err := cli.ConfigSet(cli.Context(), "maxmemory", originCacheSize).Result()
	if err != nil {
		log.Errorf(ctx, err.Error())
		return spec.ResponseFailWithFlags(spec.ActionNotSupport, "redis set max memory error: "+err.Error())
	}
	if result != STATUSOK {
		statusMsg := fmt.Sprintf("redis command status is %s", result)
		log.Errorf(ctx, statusMsg)
		return spec.ResponseFailWithFlags(spec.ActionNotSupport, "redis set max memory error: "+statusMsg)
	}

	return nil
}

func (cle *CacheLimitExecutor) start(ctx context.Context, cli *redis.Client, percentStr string, originCacheSize string, sizeStr string) *spec.Response {
	var cacheSize string
	if percentStr != "" {
		percentage, err := strconv.ParseFloat(percentStr[0:len(percentStr)-1], 64)
		if err != nil {
			log.Errorf(ctx, err.Error())
			return spec.ResponseFailWithFlags(spec.ActionNotSupport, "str parse float error: "+err.Error())
		}
		originCacheSize, err := strconv.ParseFloat(originCacheSize, 64)
		if err != nil {
			log.Errorf(ctx, err.Error())
			return spec.ResponseFailWithFlags(spec.ActionNotSupport, "str parse float error: "+err.Error())
		}
		cacheSize = fmt.Sprint(int(math.Floor(originCacheSize / 100.0 * percentage)))
	} else {
		cacheSize = sizeStr
	}

	result, err := cli.ConfigSet(cli.Context(), "maxmemory", cacheSize).Result()
	if err != nil {
		log.Errorf(ctx, err.Error())
		return spec.ResponseFailWithFlags(spec.ActionNotSupport, "redis set max memory error: "+err.Error())
	}
	if result != STATUSOK {
		statusMsg := fmt.Sprintf("redis command status is %s", result)
		log.Errorf(ctx, statusMsg)
		return spec.ResponseFailWithFlags(spec.ActionNotSupport, "redis set max memory error: "+statusMsg)
	}

	return nil
}
