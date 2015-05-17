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
#include "../../Bricks/graph/gnuplot.h"
#include "../../Bricks/strings/printf.h"

#include "../../Bricks/dflags/dflags.h"
#include "../../Bricks/3party/gtest/gtest-main-with-dflags.h"

#include "iris.h"
CEREAL_REGISTER_TYPE(LabeledFlower);

using namespace bricks::strings;
using namespace bricks::gnuplot;
using namespace yoda;

DEFINE_int32(iris_port, 3000, "");

DEFINE_bool(run, false, "Set to true to run indefinitely.");

// Flower ID, auto-increasing for test purposes.
size_t number_of_flowers = 0;
std::map<size_t, std::string> dimension_names;

TEST(Iris, Demo) {
  typedef API<KeyEntry<LabeledFlower>> TestAPI;
  TestAPI api("labeled_flowers");

  api.AsyncAdd(LabeledFlower(42, 0.1, 0.1, 0.1, 0.1, "The Answer")).Wait();

  // Ref.: http://localhost:3000/stream
  api.ExposeViaHTTP(FLAGS_iris_port, "/stream");
  const std::string Z = "";  // For `clang-format`-indentation purposes.
  EXPECT_EQ(Z,
  /*
  JSON(WithBaseType<Padawan>(KeyValueEntry(2, 0.50)), "entry") + '\n' +
                JSON(WithBaseType<Padawan>(KeyValueEntry(3, 0.33)), "entry") + '\n' +
                JSON(WithBaseType<Padawan>(KeyValueEntry(4, 0.25)), "entry") + '\n' +
                JSON(WithBaseType<Padawan>(KeyValueEntry(5, 0.20)), "entry") + '\n' +
                JSON(WithBaseType<Padawan>(KeyValueEntry(6, 0.17)), "entry") + '\n' +
                JSON(WithBaseType<Padawan>(KeyValueEntry(7, 0.76)), "entry") + '\n',
                */
            HTTP(GET(Printf("http://localhost:%d/stream?cap=1", FLAGS_iris_port))).body);


  HTTP(FLAGS_iris_port).Register("/import", [&api](Request request) {
    EXPECT_EQ("POST", request.method);
    const std::string data = request.body;
    api.Call([data](TestAPI::T_CONTAINER_WRAPPER cw) {
               auto mutable_flowers = cw.GetMutator<KeyEntry<LabeledFlower>>();
               // Skip the first line with labels.
               bool first_line = true;
               for (auto flower_definition_line : Split<ByLines>(data)) {
                 std::vector<std::string> flower_definition_fields = Split(flower_definition_line, '\t');
                 assert(flower_definition_fields.size() == 5u);
                 if (first_line && flower_definition_fields.back() == "Label") {
                   dimension_names.clear();
                   for (size_t i = 0; i < flower_definition_fields.size() - 1; ++i) {
                     dimension_names[i] = flower_definition_fields[i];
                   }
                   continue;
                 }
                 first_line = false;
                 // Parse flower data and add it.
                 mutable_flowers.Add(LabeledFlower(++number_of_flowers,
                                                   FromString<double>(flower_definition_fields[0]),
                                                   FromString<double>(flower_definition_fields[1]),
                                                   FromString<double>(flower_definition_fields[2]),
                                                   FromString<double>(flower_definition_fields[3]),
                                                   flower_definition_fields[4]));
               }
               return Printf("Successfully imported %d flowers.\n", static_cast<int>(number_of_flowers));
             },
             std::move(request));
  });

  EXPECT_EQ("Successfully imported 150 flowers.\n",
            HTTP(POSTFromFile(Printf("http://localhost:%d/import", FLAGS_iris_port), "dataset.tsv", "text/tsv"))
                .body);

  /*
  struct Dima {
    bool Entry(std::unique_ptr<Padawan>& e, size_t, size_t) {
      std::cerr << "Dima::Entry!\n";
      return true;
    }
  };
  Dima dima;
  api.Subscribe(dima);
  */

  if (FLAGS_run) {
    // Ref.: http://localhost:3000/get?id=42
    HTTP(FLAGS_iris_port).Register("/get", [&api](Request request) {
      const auto id = FromString<int>(request.url.query["id"]);
      // TODO(dk+mz): Re-enable simple syntax?
      api.Call([id](TestAPI::T_CONTAINER_WRAPPER cw) { return cw.GetAccessor<KeyEntry<LabeledFlower>>()[id]; },
               std::move(request));
    });

    // Ref.: [POST] http://localhost:3000/add?label=setosa&sl=5&sw=5&pl=5&pw=5
    HTTP(FLAGS_iris_port).Register("/add", [&api](Request request) {
      const std::string label = request.url.query["label"];
      const auto sl = FromString<double>(request.url.query["sl"]);
      const auto sw = FromString<double>(request.url.query["sw"]);
      const auto pl = FromString<double>(request.url.query["pl"]);
      const auto pw = FromString<double>(request.url.query["pw"]);
      // In real life this should be a POST.
      if (!label.empty()) {
        LabeledFlower flower(++number_of_flowers, sl, sw, pl, pw, label);
        api.Call([flower](TestAPI::T_CONTAINER_WRAPPER cw) {
                   // TODO(dk+mz): Re-enable simple syntax?
                   cw.GetMutator<KeyEntry<LabeledFlower>>().Add(flower);
                   return "OK\n";
                 },
                 std::move(request));
      } else {
        request("Need non-empty label, as well as sl/sw/pl/pw.\n");
      }
    });

    // Ref.: http://localhost:3000/viz
    // Ref.: http://localhost:3000/viz?x=1&y=2
    HTTP(FLAGS_iris_port).Register("/viz", [&api](Request request) {
      auto x_dim = std::min(size_t(3), FromString<size_t>(request.url.query.get("x", "0")));
      auto y_dim = std::min(size_t(3), FromString<size_t>(request.url.query.get("y", "1")));
      if (y_dim == x_dim) {
        y_dim = (x_dim + 1) % 4;
      }
      struct PlotIrises {
        struct Data {
          std::string x_label;
          std::string y_label;
          std::map<std::string, std::vector<std::pair<double, double>>> labeled_flowers;
        };
        Request request;
        explicit PlotIrises(Request request) : request(std::move(request)) {}
        void operator()(Data data) {
          GNUPlot graph;
          graph.Title("Iris flower data set.")
              .Grid("back")
              .XLabel(data.x_label)
              .YLabel(data.y_label)
              .ImageSize(800)
              .OutputFormat("pngcairo");
          for (const auto& per_label : data.labeled_flowers) {
            graph.Plot(WithMeta([&per_label](Plotter& p) {
                                  for (const auto& xy : per_label.second) {
                                    p(xy.first, xy.second);
                                  }
                                })
                           .AsPoints()
                           .Name(per_label.first));
          }
          request(graph);
        }
      };
      api.Call([x_dim, y_dim](TestAPI::T_CONTAINER_WRAPPER cw) {
                 const auto flowers = cw.GetAccessor<KeyEntry<LabeledFlower>>();
                 PlotIrises::Data data;
                 for (size_t i = 1; i <= number_of_flowers; ++i) {
                   // TODO(dk+mz): Re-enable simple syntax?
                   // const LabeledFlower& flower = cw.GetAccessor<KeyEntry<LabeledFlower>>()[i];
                   const LabeledFlower& flower = flowers[i];
                   data.labeled_flowers[flower.label].emplace_back(flower.x[x_dim], flower.x[y_dim]);
                 }
                 data.x_label = dimension_names[x_dim];
                 data.y_label = dimension_names[y_dim];
                 return data;
               },
               PlotIrises(std::move(request)));
    });

    HTTP(FLAGS_iris_port).Join();
  }
}