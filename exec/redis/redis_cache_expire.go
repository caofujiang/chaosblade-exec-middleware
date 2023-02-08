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
	"github.com/go-redis/redis/v8"
	"time"

	"github.com/chaosblade-io/chaosblade-exec-os/exec/category"
	"github.com/chaosblade-io/chaosblade-spec-go/log"
	"github.com/chaosblade-io/chaosblade-spec-go/spec"
)

const CacheExpireBin = "chaos_cacheExpire"

type CacheExpireActionCommandSpec struct {
	spec.BaseExpActionCommandSpec
}

func NewCacheExpireActionSpec() spec.ExpActionCommandSpec {
	return &CacheExpireActionCommandSpec{
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
					Name: "key",
					Desc: "The key to be set a expiration, default expire all keys",
				},
				&spec.ExpFlag{
					Name: "expiration",
					Desc: `The expiration of the key. A expiration string should be able to be converted to a time duration, such as "5s" or "30m"`,
				},
				&spec.ExpFlag{
					Name: "option",
					Desc: "The additional options of expiration, only NX, XX, GT, LT supported",
				},
			},
			ActionExecutor: &CacheExpireExecutor{},
			ActionExample: `
# expire all keys
blade create redis cache-expire --addr 127.0.0.1:6379 --option GT --expiration 1m
`,
			ActionPrograms:   []string{CacheExpireBin},
			ActionCategories: []string{category.SystemTime},
		},
	}
}

func (*CacheExpireActionCommandSpec) Name() string {
	return "cache-expire"
}

func (*CacheExpireActionCommandSpec) Aliases() []string {
	return []string{"ce"}
}

func (*CacheExpireActionCommandSpec) ShortDesc() string {
	return "Cache Expire"
}

func (k *CacheExpireActionCommandSpec) LongDesc() string {
	if k.ActionLongDesc != "" {
		return k.ActionLongDesc
	}
	return "Expire keys in Redis"
}

func (*CacheExpireActionCommandSpec) Categories() []string {
	return []string{category.SystemProcess}
}

type CacheExpireExecutor struct {
	channel spec.Channel
}

func (cee *CacheExpireExecutor) Name() string {
	return "cache-expire"
}

func (cee *CacheExpireExecutor) Exec(uid string, ctx context.Context, model *spec.ExpModel) *spec.Response {
	addrStr := model.ActionFlags["addr"]
	passwordStr := model.ActionFlags["password"]
	keyStr := model.ActionFlags["key"]
	expirationStr := model.ActionFlags["expiration"]
	optionStr := model.ActionFlags["option"]

	cli := redis.NewClient(&redis.Options{
		Addr:     addrStr,
		Password: passwordStr,
	})
	_, err := cli.Ping(cli.Context()).Result()
	if err != nil {
		log.Errorf(ctx, err.Error())
		return spec.ResponseFailWithFlags(spec.ActionNotSupport, "redis ping error: "+err.Error())
	}

	return cee.start(ctx, cli, keyStr, expirationStr, optionStr)
}

func (cee *CacheExpireExecutor) SetChannel(channel spec.Channel) {
	cee.channel = channel
}

func ExpireFunc(cli *redis.Client, key string, expiration time.Duration, option string) *redis.BoolCmd {
	switch option {
	case OPTIONNX:
		return cli.ExpireNX(cli.Context(), key, expiration)
	case OPTIONXX:
		return cli.ExpireXX(cli.Context(), key, expiration)
	case OPTIONGT:
		return cli.ExpireGT(cli.Context(), key, expiration)
	case OPTIONLT:
		return cli.ExpireLT(cli.Context(), key, expiration)
	default:
		return cli.Expire(cli.Context(), key, expiration)
	}
}

func (cee *CacheExpireExecutor) start(ctx context.Context, cli *redis.Client, keyStr string, expirationStr string, optionStr string) *spec.Response {
	expiration, err := time.ParseDuration(expirationStr)
	if err != nil {
		log.Errorf(ctx, err.Error())
		return spec.ResponseFailWithFlags(spec.ActionNotSupport, "parse duration error: "+err.Error())
	}

	if keyStr == "" {
		// Get all keys from the server
		allKeys, err := cli.Keys(cli.Context(), "*").Result()
		if err != nil {
			log.Errorf(ctx, err.Error())
			return spec.ResponseFailWithFlags(spec.ActionNotSupport, "redis get all keys error: "+err.Error())
		}

		for _, key := range allKeys {
			result, err := ExpireFunc(cli, key, expiration, optionStr).Result()
			if err != nil {
				log.Errorf(ctx, err.Error())
				return spec.ResponseFailWithFlags(spec.ActionNotSupport, "redis expire key error: "+err.Error())
			}
			if !result {
				log.Errorf(ctx, "redis expire key failed")
				return spec.ResponseFailWithFlags(spec.ActionNotSupport, "redis expire key failed")
			}
		}
	} else {
		result, err := ExpireFunc(cli, keyStr, expiration, optionStr).Result()
		if err != nil {
			log.Errorf(ctx, err.Error())
			return spec.ResponseFailWithFlags(spec.ActionNotSupport, "redis expire key error: "+err.Error())
		}
		if !result {
			log.Errorf(ctx, "redis expire key failed")
			return spec.ResponseFailWithFlags(spec.ActionNotSupport, "redis expire key failed")
		}
	}

	return nil
}
