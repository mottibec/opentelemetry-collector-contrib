// Copyright  The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tailsamplingprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor"

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/sampling"
)

func getNewCombinedPolicy(logger *zap.Logger, config CombinedCfg) (sampling.PolicyEvaluator, error) {
	var subPolicyEvaluators []sampling.PolicyEvaluator
	for i := range config.SubPolicyCfg {
		policyCfg := config.SubPolicyCfg[i]
		policy, _ := getCombinedSubPolicyEvaluator(logger, &policyCfg)
		subPolicyEvaluators = append(subPolicyEvaluators, policy)
	}
	return sampling.NewCombined(logger, subPolicyEvaluators), nil
}

// Return instance of combined sub-policy
func getCombinedSubPolicyEvaluator(logger *zap.Logger, cfg *CombinedSubPolicyCfg) (sampling.PolicyEvaluator, error) {
	switch cfg.Type {
	case AlwaysSample:
		return sampling.NewAlwaysSample(logger), nil
	case NumericAttribute:
		nafCfg := cfg.NumericAttributeCfg
		return sampling.NewNumericAttributeFilter(logger, nafCfg.Key, nafCfg.MinValue, nafCfg.MaxValue), nil
	case StringAttribute:
		safCfg := cfg.StringAttributeCfg
		return sampling.NewStringAttributeFilter(logger, safCfg.Key, safCfg.Values, safCfg.EnabledRegexMatching, safCfg.CacheMaxSize, safCfg.InvertMatch), nil
	case RateLimiting:
		rlfCfg := cfg.RateLimitingCfg
		return sampling.NewRateLimiting(logger, rlfCfg.SpansPerSecond), nil
	case StatusCode:
		return sampling.NewStatusCodeFilter(logger, cfg.StatusCodeCfg.StatusCodes)
	default:
		return nil, fmt.Errorf("unknown sampling policy type %s", cfg.Type)
	}
}
