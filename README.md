# LaTiS 3

LaTiS is both a library and a server application that enables data access from many sources, between many formats, and through many interfaces using a novel data model.

## Getting Started

We use [SBT](https://www.scala-sbt.org/) to manage our build. Until we begin making releases, you will need to [install SBT](#sbt) to use LaTiS.

To get an idea of what using LaTiS is like, see [Running the Example](#example).

To set up your own LaTiS instance, see [Running with Docker](#docker) or [Running with an Executable JAR](#jar).

### <a name="sbt"></a> Installing SBT

The recommended way to install SBT is to first install [Coursier](https://get-coursier.io/), an application and artifact manager for Scala, and then run `cs setup`. See the [Couriser documentation](https://get-coursier.io/docs/cli-installation) for installation instructions.

See the [SBT documentation](https://www.scala-sbt.org/download/) for more ways to install SBT.

If you use [Nix](https://nixos.org/), just run `nix develop` to enter a development environment that includes SBT.

### <a name="example"></a> Running the Example

You will need to clone this repository and have [SBT installed](#sbt) to run the example.

#### Starting the Instance

First run `sbt`. Once in the SBT shell, run the following to start the example server:

```
sbt:latis3> example/reStart
```

This will start a LaTiS service with a DAP 2 interface. This interface implements most of the [OPeNDAP Data Access Protocol, version 2](https://zenodo.org/records/10794666). LaTiS supports multiple interfaces, including custom interfaces, so you can choose whichever data access API best suites your needs. In this example we will look at the kinds of queries you can make through the DAP 2 interface.

#### Catalogs

We refer to the set of datasets available from a LaTiS instance as the instance's catalog. Run the following command to get the catalog for your example instance:

```sh
curl http://localhost:8080/dap2/
```

You should get back a JSON response that looks something like this (pretty-printed here for readability):

```json
{
  "dataset": [
    {"identifier": "example_dataset", "title": "Example Dataset"}
  ]
}
```

Each object in the `dataset` array corresponds to a dataset that is available through LaTiS. The `identifier` is a unique identifier that we use to refer to datasets in queries.

If you visit the catalog endpoint in your browser or make a request with a `Accept: text/html` header, you will receive an HTML rendering of the catalog instead.

The default catalog is constructed from a directory of FDML files, which are XML files that describe the structure, location, and format of datasets to LaTiS. An in-depth description of the FDML format is forthcoming, but for now you can see the FDML files being used in the `example/datasets/fdml` directory. LaTiS also supports custom catalogs that construct datasets from other sources, like database tables, self-describing file formats, or other data access APIs.

#### Datasets

A dataset is a collection of data and metadata that can be queried, operated on, and written out in a variety of formats.

A query is made using the dataset's identifier and an extension indicating the output format. This query will return the dataset's metadata:

```sh
curl http://localhost:8080/dap2/example_dataset.meta
```

You should get a response like this (pretty-printed here for readability):

```json
{
  "id": "example_dataset",
  "title": "Example Dataset",
  "history": "",
  "model": "time -> (temperature, wind_direction, wind_speed)",
  "variable": [
    {
      "id": "time",
      "type": "string",
      "class": "latis.time.Time",
      "units": "yyyy-MM-dd"
    },
    {
      "id": "temperature",
      "type": "int",
      "units": "degree Fahrenheit"
    },
    {
      "id": "wind_direction",
      "type": "int",
      "units": "degrees"
    },
    {
      "id": "wind_speed",
      "type": "int",
      "units": "knots"
    }
  ]
}
```

Dataset metadata includes the set of variables in the dataset and metadata about each variable, like units and data types, and the relationship between the variables, which we capture in the `model` field. This example dataset is temperature, wind direction, and wind speed as a function of time (as indicated by the `->` arrow).

More documentation about the data model used by LaTiS is forthcoming, but the data model is part of what makes LaTiS so flexible.

These queries will return the data in different formats:

```sh
curl http://localhost:8080/dap2/example_dataset.csv
```

```
time,temperature,wind_direction,wind_speed
2025-01-01,60,0,10
2025-01-02,65,180,5
2025-01-03,70,270,15
2025-01-04,70,0,5
2025-01-05,65,90,5
2025-01-06,65,45,10
2025-01-07,60,135,20
2025-01-08,50,225,15
2025-01-09,45,315,5
2025-01-10,40,0,5
```

```sh
curl http://localhost:8080/dap2/example_dataset.jsonl
```

```
["2025-01-01",60,0,10]
["2025-01-02",65,180,5]
["2025-01-03",70,270,15]
["2025-01-04",70,0,5]
["2025-01-05",65,90,5]
["2025-01-06",65,45,10]
["2025-01-07",60,135,20]
["2025-01-08",50,225,15]
["2025-01-09",45,315,5]
["2025-01-10",40,0,5]
```

LaTiS supports several output formats out of the box. You can also define your own.

The following sections describe how to filter and operate on datasets.

#### Projections

A projection allows you to limit the set of variables returned by a query. Only variables that appear in a projection will appear in the result. Projecting variable that do not appear in the dataset will result in an error.

A projection is written as a comma-separated list of the variable names to keep in the result. This will give you the temperature as a function of time:

```sh
curl 'http://localhost:8080/dap2/example_dataset.csv?time,temperature'
```

```
time,temperature
2025-01-01,60
2025-01-02,65
2025-01-03,70
2025-01-04,70
2025-01-05,65
2025-01-06,65
2025-01-07,60
2025-01-08,50
2025-01-09,45
2025-01-10,40
```

This will give you the wind direction and speed as a function of time:

```sh
curl 'http://localhost:8080/dap2/example_dataset.csv?time,wind_direction,wind_speed'
```

```
time,wind_direction,wind_speed
2025-01-01,0,10
2025-01-02,180,5
2025-01-03,270,15
2025-01-04,0,5
2025-01-05,90,5
2025-01-06,45,10
2025-01-07,135,20
2025-01-08,225,15
2025-01-09,315,5
2025-01-10,0,5
```

#### Selections

A selection allows you to subset a dataset by the values of variables. A query can contain multiple selections. Only data that satisfies all of the selections will be included in the results.

A selection is written as a variable name, a comparison operator, and a comparison value. Common comparison operators are `>`, `>=`, `<`, and `<=`. Note that `>` and `<` are not valid characters in URLs and must be encoded to `%3E` and `%3C` respectively.

You can select on time to get data for certain days:

```sh
curl 'http://localhost:8080/dap2/example_dataset.csv?time%3E=2025-01-03&time%3C2025-01-05'
```

```
time,temperature,wind_direction,wind_speed
2025-01-03,70,270,15
2025-01-04,70,0,5
```

You can also select on multiple variables:

```sh
curl 'http://localhost:8080/dap2/example_dataset.csv?wind_speed%3E=15&wind_direction%3E=90&wind_direction%3C=270'
```

```
time,temperature,wind_direction,wind_speed
2025-01-03,70,270,15
2025-01-07,60,135,20
2025-01-08,50,225,15
```

#### Time Conversion

LaTiS can convert between time formats so you can get data in your desired time format regardless of the format used by the original dataset.

You can convert times to different units using the `convertTime` operation:

```sh
curl 'http://localhost:8080/dap2/example_dataset.csv?convertTime(%22days%20since%202025-01-01%22)'
```

```
time,temperature,wind_direction,wind_speed
0.0,60,0,10
1.0,65,180,5
2.0,70,270,15
3.0,70,0,5
4.0,65,90,5
5.0,65,45,10
6.0,60,135,20
7.0,50,225,15
8.0,45,315,5
9.0,40,0,5
```

You can change how formatted times are displayed using the `formatTime` operation:

```sh
curl 'http://localhost:8080/dap2/example_dataset.csv?formatTime(%22yyyy-DDD%22)'
```

```
2025-001,60,0,10
2025-002,65,180,5
2025-003,70,270,15
2025-004,70,0,5
2025-005,65,90,5
2025-006,65,45,10
2025-007,60,135,20
2025-008,50,225,15
2025-009,45,315,5
2025-010,40,0,5
```

Note that double quotes and spaces are not allowed in URLs, so they are encoded to `%22` and `%20` respectively.

#### Other Operations

LaTiS supports a number of other operations for manipulating datasets. You can also define your own. Here are some examples.

The `take` operation returns only the first N values:

```sh
curl 'http://localhost:8080/dap2/example_dataset.csv?take(3)'
```

```
time,temperature,wind_direction,wind_speed
2025-01-01,60,0,10
2025-01-02,65,180,5
2025-01-03,70,270,15
```

The `rename` operation renames variables:

```sh
curl 'http://localhost:8080/dap2/example_dataset.csv?rename(temperature,t)&rename(wind_speed,s)&rename(wind_direction,d)'
```

```
time,t,d,s
2025-01-01,60,0,10
2025-01-02,65,180,5
2025-01-03,70,270,15
2025-01-04,70,0,5
2025-01-05,65,90,5
2025-01-06,65,45,10
2025-01-07,60,135,20
2025-01-08,50,225,15
2025-01-09,45,315,5
2025-01-10,40,0,5
```

The `countBy` operation returns how many times a value appears in a dataset:

```sh
curl 'http://localhost:8080/dap2/example_dataset.csv?countBy(temperature)'
```

```
temperature,count
40,1
45,1
50,1
60,2
65,3
70,2
```

#### Stopping the Instance

Run the following to stop the example server:

```
sbt:latis3> example/reStop
```

### <a name="docker"></a> Running with Docker

You will need to clone this repository and have [SBT installed](#sbt) to build a Docker image.

Run the following to create the Docker image:

```sh
sbt server/docker
```

Then run something like the following to run the server:

```sh
docker run -p 8080:8080 --mount type=bind,src=<dataset-dir>,dst=/datasets/fdml io.latis-data/latis3:0.1.0-SNAPSHOT
```

`<dataset-dir>` should be replaced with the absolute path to a directory of FDML files.

### <a name="jar"></a> Running with an Executable JAR

You will need to clone this repository and have [SBT installed](#sbt) to build an executable JAR.

Run the following to create the executable JAR:

```sh
sbt server/assembly
```

The second to last line of the output (starting with "Built") tells you where the file was placed.

Then run something like the following to run the server:

```sh
java -Dlatis.fdml.dir=<dataset-dir> -jar <path-to-jar>
```

`<dataset-dir>` should be replaced with the path to a directory of FDML files.
