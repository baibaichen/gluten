/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "utils/Common.h"

#include <gtest/gtest.h>

namespace gluten {

TEST(RegexValidationTest, RejectsSmallUnsupportedPatterns) {
  for (const auto* pattern : {"[a-z", R"((\w+)\s+\1)", "(?<=foo)(bar)", "(foo)(?=bar)", R"((abc)\1)", "(?<=X)"}) {
    SCOPED_TRACE(pattern);
    std::string error;
    EXPECT_FALSE(validatePattern(pattern, error));
    EXPECT_NE(error.find(pattern), std::string::npos);
  }
}

TEST(RegexValidationTest, AcceptsSupportedPatterns) {
  for (const auto* pattern : {"", "foo", "^foo$", "[a-z]+", R"(\d+)", "\xE4\xBD\xA0\xE5\xA5\xBD"}) {
    SCOPED_TRACE(pattern);
    std::string error;
    EXPECT_TRUE(validatePattern(pattern, error));
    EXPECT_TRUE(error.empty());
  }
}

TEST(RegexValidationTest, RejectsIncompatibleCharacterClasses) {
  for (const auto* pattern : {"[a[b]]", "[a&&[b]]", "[a&&[^b]]"}) {
    SCOPED_TRACE(pattern);
    std::string error;
    EXPECT_FALSE(validatePattern(pattern, error));
    EXPECT_FALSE(error.empty());
  }
}

} // namespace gluten
