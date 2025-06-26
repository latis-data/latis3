# Constructing a LaTiS URL

## LaTiS URL
This is a LaTiS URL:

`https://lasp.colorado.edu/lisird/latis/dap/dataset.suffix?projection&selection&operation`

Where the following variables have a multitude of options based on the data you are hoping to access:
* dataset
* suffix
* projections
* selection
* operation

Below we walk through the different options you have for each of the above variables. 
Jump to:
* [suffix](#suffix-options)
* [operation](#operation-options)

## Suffix options

| Suffix | Description                                                             |
|--------|-------------------------------------------------------------------------|
| .asc   | ASCII representation reflecting how the dataset is modeled              |
| .bin   | Binary stream of IEEE bytes                                             |
| .csv   | Comma delimited ASCII with simple header                                |
| .das   | DAP2 standard Dataset Attribute Structure with metadata only            |
| .dds   | DAP2 standard Dataset Descriptor Structure with structure metadata only |
| .dods  | DAP2 standard data output                                               |
| .html  | Brief HTML landing page for a dataset                                   |
| .json  | JSON with labels                                                        |
| .jsona | JSON as arrays                                                          |
| .jsond | JSON with metadata and arrays of data                                   |
| .nc    | NetCDF file                                                             |
| .tab   | Tab delimited ASCII with no header                                      |
| .txt   | Comma delimited ASCII with no header                                    |
| .zip   | Zip file. Available only for file list datasets                         | 


## Operation options
| Operation          | Description                                                                                        | Usage                                                                                                          |
|--------------------|----------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| convert_time       | Convert time values to the given units.                                                            | `&convert_time(time,units)`, where units is duration units since an epoch (e.g. milliseconds since 1970-01-01) |
| drop               | Return all but the first n samples.                                                                | ``&drop(n)``                                                                                                   |
| exclude_missing    | Exclude all samples that contain a missing value                                                   | `&exclude_missing()`                                                                                           |
| first              | Return only the first sample.                                                                      | `&first()`                                                                                                     |
| format_time        | Convert time values to the given format (for text output) as specified by Java's SimpleDateFormat. | `&format_time(format)`, e.g. `&format_time(yyyy-MM-dd'T'HH:mm:ss.SSS)`                                         |
| irradianceAtPlanet | Compute the irradiance at a planet.                                                                | `&irradianceAtPlanet(irradiance_variable,planet)`                                                              |
| last               | Return only the last sample.                                                                       | `&last()  `                                                                                                    |
| limit              | Return the first n samples.                                                                        | `&limit(n)   `                                                                                                 |
| rename             | Change the name of a variable given its original and new name.                                     | `&rename(orig,new)`                                                                                            |
| replace_missing    | Replace any missing value with the given value.                                                    | `&replace_missing(value)`                                                                                      |
| stride             | Return every nth sample.                                                                           | `&stride(n)`                                                                                                   |
| take               | Return the first n samples.                                                                        | `&take(n)  `                                                                                                   |
| take_right         | Return the last n samples.                                                                         | `&take_right(n)`                                                                                               |



