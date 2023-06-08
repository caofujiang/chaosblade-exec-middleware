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
	"github.com/howeyc/gopass"
	"strconv"
)

const CachePenetrateBin = "chaos_cachePenetrate"

type CachePenetrateActionCommandSpec struct {
	spec.BaseExpActionCommandSpec
}

func NewCachePenetrateActionSpec() spec.ExpActionCommandSpec {
	return &CachePenetrateActionCommandSpec{
		spec.BaseExpActionCommandSpec{
			ActionMatchers: []spec.ExpFlagSpec{},
			ActionFlags: []spec.ExpFlagSpec{
				&spec.ExpFlag{
					Name: "addr",
					Desc: "The address of redis server",
				},
				&spec.ExpFlag{
					Name: "request-num",
					Desc: "The number of requests",
				},
			},
			ActionExecutor: &CachePenetrateExecutor{},
			ActionExample: `
# 10000 request
blade create redis cache-penetrate --addr 192.168.56.101:6379 --password 123456 --request-num 10000
`,
			ActionPrograms:   []string{CachePenetrateBin},
			ActionCategories: []string{category.SystemTime},
		},
	}
}

func (*CachePenetrateActionCommandSpec) Name() string {
	return "cache-penetrate"
}

func (*CachePenetrateActionCommandSpec) Aliases() []string {
	return []string{"cp"}
}

func (*CachePenetrateActionCommandSpec) ShortDesc() string {
	return "Cache Penetrate"
}

func (k *CachePenetrateActionCommandSpec) LongDesc() string {
	if k.ActionLongDesc != "" {
		return k.ActionLongDesc
	}
	return "Penetrate cache"
}

func (*CachePenetrateActionCommandSpec) Categories() []string {
	return []string{category.SystemProcess}
}

type CachePenetrateExecutor struct {
	channel spec.Channel
}

func (cpe *CachePenetrateExecutor) Name() string {
	return "cache-penetrate"
}

func (cpe *CachePenetrateExecutor) Exec(uid string, ctx context.Context, model *spec.ExpModel) *spec.Response {
	addrStr := model.ActionFlags["addr"]
	requestNumStr := model.ActionFlags["request-num"]

	if _, ok := spec.IsDestroy(ctx); ok {
		return spec.ReturnSuccess("destroy cache penetrate success")
	}

	fmt.Print("Please enter the password of redis server:")
	passwordStr, err := gopass.GetPasswd()
	if err != nil {
		log.Errorf(ctx, "password is illegal, err: %s", err.Error())
		return spec.ResponseFailWithFlags(spec.ParameterIllegal, "password", "****", err.Error())
	}

	cli := redis.NewClient(&redis.Options{
		Addr:     addrStr,
		Password: string(passwordStr),
	})
	_, err = cli.Ping(cli.Context()).Result()
	if err != nil {
		errMsg := "redis ping error: " + err.Error()
		log.Errorf(ctx, errMsg)
		return spec.ResponseFailWithFlags(spec.ActionNotSupport, errMsg)
	}

	return cpe.start(ctx, cli, requestNumStr)

}

func (cpe *CachePenetrateExecutor) SetChannel(channel spec.Channel) {
	cpe.channel = channel
}

func (cpe *CachePenetrateExecutor) start(ctx context.Context, cli *redis.Client, requestNumStr string) *spec.Response {
	requestNum, err := strconv.Atoi(requestNumStr)
	if err != nil {
		errMsg := "string to int error: " + err.Error()
		log.Errorf(ctx, errMsg)
		return spec.ResponseFailWithFlags(spec.ActionNotSupport, errMsg)
	}

	pipe := cli.Pipeline()
	for i := 0; i < requestNum; i++ {
		pipe.Get(cli.Context(), "CM_Chaos_cbF3bNw9klHv")
	}
	_, err = pipe.Exec(cli.Context())
	if err != redis.Nil {
		errMsg := "redis pipe exec error: " + err.Error()
		log.Errorf(ctx, errMsg)
		return spec.ResponseFailWithFlags(spec.ActionNotSupport, errMsg)
	}

	return spec.ReturnSuccess("cache penetrate success")
}
