# LaTiS 3

LaTiS is a software framework designed for accessing, processing, and outputting scientific data. It provides a unified 
data access and query interface, making it easier to work with a wide range of datasets. The key features of LaTiS 
are as follows:

- Functional Data Model: LaTiS uses a mathematical model to describe and manipulate datasets, enabling broad data interoperability
- Modular Architecture: LaTiS supports reusable and custom readers, operations, and writers to handle data from various sources and formats
- Web Service Layer: LaTiS includes a web service interface for requesting data using standard query syntax and data protocols

LaTiS arose out of a need for unified web service access to a variety of scientific data sources. Instead of managing a 
centralized repository of data, LaTiS provides a data access layer by adapting existing data sources to a common data model.
LaTiS is used in various scientific applications, including space missions and data communities, to provide seamless access to
complex datasets.

If you are looking for how LaTiS has served complex data to a web application, check out either of the following sites 
where LaTiS is used as the data access layer:
- https://lasp.colorado.edu/lisird/
- https://lasp.colorado.edu/space-weather-portal/data

LaTiS is currently maintained at the [Laboratory for Atmospheric and Space Physics](https://lasp.colorado.edu), a research 
institution at the University of Colorado at Boulder. 

### How do I get me one of those? 

#### LaTiS servers

LaTiS is currently operated through servers. Each dataset has its own server that handles the data ingestion and easy access
by the LaTiS API. There are currently ___ LaTiS servers for ___ datasets. A comprehensive list of current servers can be 
found [here](TODO: does such a list exist?). If you are looking to get a new LaTiS server set up for a dataset, TBD.

#### LaTiS URLs

All datasets currently available via a publicly accessible LaTiS server can be accessed by a simple API query that takes 
the form of a LaTiS URL. This URL takes the following form:

`https://lasp.colorado.edu/lisird/latis/dap/dataset.suffix?projection&selection&operation`

Where the following variables have a multitude of options based on the data you are hoping to access:
* dataset
* suffix
* projections
* selection
* operation

More information on constructing a LaTiS URL can be found [here](./docs/constructing_a_latis_url.md). 

In addition to visiting LaTiS URLs directly, data can be downloaded using `wget` or `curl` in the command line. The 
syntax is shown below, where `<LaTiS URL>` is any valid LaTiS URL and `<filename>` is any valid file name with the 
appropriate file extension (i.e., an extension that matches the requested `suffix`). The URL is always surrounded by quotes.

```
wget -O <filename> "<LaTiS URL>"

curl "<LaTiS URL>" > <filename>
```

Example wget Commands:

```
wget -O data.csv "https://lasp.colorado.edu/lisird/latis/dap/uars_solstice_ssi.csv?time,wavelength,irradiance&time<2001&limit(10)"

wget -O data.txt "https://lasp.colorado.edu/lisird/latis/dap/sorce_tsi_24hr_l3.asc?time>=2005-05-05T12:00&time<2006-05-05T12:00"
```
Example curl Commands:

```
curl "https://lasp.colorado.edu/lisird/latis/dap/fism_daily_files.zip" > data.zip

curl "https://lasp.colorado.edu/lisird/latis/dap/nrl2_tsi_P1Y.json?replace_missing(NaN)&format_time(yyyy)" > data.json
```

## Contributing to the LaTiS project

LaTiS is a very low-funded project which means that we welcome contributors from elsewhere! One of the biggest challenges
is lowering the barrier to entry for folks interested in setting up their own LaTiS server. This is something we are
currently working on. 

In order to contribute to LaTiS, you will need to install and build it. 

### Some Notes Before Getting Started

#### Scala

LaTiS is written in the [Scala](https://www.scala-lang.org/) programming language. While most software projects in the 
space physics community are moving towards using Python, we chose to write LaTiS in scala for its strong typing and speed. 
Knowing that the majority of the users we interact with use Python, we have investigated Python integration with LaTiS using
[Jep](https://github.com/ninia/jep) and [ScalaPy](https://scalapy.dev/). More documentation on how to use Python code in
LaTiS, see [Using Python with LaTiS](./docs/latis_python_integration.md).

#### LaTiS Versions

This repository houses LaTiS 3. LaTiS 3 is currently in development and we are working to complete the transition from
LaTiS 2 to LaTiS 3, as such, we have not made any releases of LaTiS3, yet. The installation instructions below take this
into account and require that the build tool we use (SBT) will be required until releases are made.

### Getting Started

We use [SBT](https://www.scala-sbt.org/) to manage our build. Until we begin making releases, you will need to 
[install SBT](#sbt) to use LaTiS.

To get an idea of what using LaTiS is like, see [Running the Example](./docs/latis_example.md).

To set up your own LaTiS instance, see [Running with Docker](#docker) or [Running with an Executable JAR](#jar).

### <a name="sbt"></a> Installing SBT

The recommended way to install SBT is to first install [Coursier](https://get-coursier.io/), an application and artifact 
manager for Scala, and then run `cs setup`. See the [Couriser documentation](https://get-coursier.io/docs/cli-installation) 
for installation instructions. Then follow the [SBT documentation](https://www.scala-sbt.org/download/) for continuing 
with the SBT installation.

If you use [Nix](https://nixos.org/), simply run `nix develop` to enter a development environment that includes SBT.

### <a name="latis-install"></a> Building and installing LaTiS
TBA

### <a name="docker"></a> Running with Docker

You will need to clone this repository and have [SBT installed](#sbt) to build a Docker image.

Run the following to create the Docker image:

```sh
sbt server/docker
```

Then run something like (TODO: why "like"? What could be different?) the following to run the server:

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

## Contributors
Our main contributors:
* [@dlinhol](https://www.github.com/dlindhol)
* [@lindholc](https://github.com/lindholc)
