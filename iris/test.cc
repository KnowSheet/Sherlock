/*******************************************************************************
The MIT License (MIT)

Copyright (c) 2015 Dmitry "Dima" Korolev <dmitry.korolev@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*******************************************************************************/

// http://en.wikipedia.org/wiki/Iris_flower_data_set
// http://support.sas.com/documentation/cdl/en/graphref/65389/HTML/default/images/gtdshapa.png
// http://www.math.uah.edu/stat/data/Fisher.html

#include "../yoda/yoda.h"

#include "../../Bricks/net/api/api.h"
#include "../../Bricks/dflags/dflags.h"
#include "../../Bricks/3party/gtest/gtest-main-with-dflags.h"
#include "../../Bricks/strings/printf.h"

#include "iris.h"
CEREAL_REGISTER_TYPE(LabeledFlower);

using namespace bricks::strings;
using namespace yoda;

DEFINE_bool(run, false, "Set to true to run indefinitely.");
DEFINE_int32(iris_port, 3000, "");

TEST(Iris, SmokeTest) {
  typedef API<KeyEntry<LabeledFlower>> TestAPI;
  TestAPI api("labeled_flowers");

  struct RESTfulResponse {
    Request request;
    explicit RESTfulResponse(Request request) : request(std::move(request)) {}
    void operator()(const std::string& response) { request(response); }
  };

  HTTP(FLAGS_iris_port).Register("/import", [&api](Request r) {
    EXPECT_EQ("POST", r.method);
    const std::string data = r.body;
    api.Call([data](TestAPI::T_CONTAINER_WRAPPER& cw) {
               // Flower index in the file is its ID.
               // Importing flowers can and should only be done once for this test.
               size_t key = 0;
               // Skip the first line with labels.
               bool first_line = true;
               for (auto flower_definition_line : Split<ByLines>(data)) {
                 std::vector<std::string> flower_definition_fields = Split(flower_definition_line, '\t');
                 assert(flower_definition_fields.size() == 5u);
                 if (first_line && flower_definition_fields.back() == "Label") {
                   continue;
                 }
                 first_line = false;
                 // Parse flower data and add it.
                 cw.Add(LabeledFlower(++key,
                                      FromString<double>(flower_definition_fields[0]),
                                      FromString<double>(flower_definition_fields[1]),
                                      FromString<double>(flower_definition_fields[2]),
                                      FromString<double>(flower_definition_fields[3]),
                                      flower_definition_fields[4]));
               }
               return Printf("Successfully imported %d flowers.\n", static_cast<int>(key));
             },
             RESTfulResponse(std::move(r)));
  });

  EXPECT_EQ("Successfully imported 150 flowers.\n",
            HTTP(POSTFromFile(Printf("http://localhost:%d/import", FLAGS_iris_port), "dataset.tsv", "text/tsv"))
                .body);

  api.ExposeViaHTTP(FLAGS_iris_port, "/pubsub");

  if (FLAGS_run) {
    HTTP(FLAGS_iris_port).Join();
  }
}
