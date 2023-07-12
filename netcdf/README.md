# latis3-netcdf

The latis3-netcdf subproject provides support for reading and writing netCDF files using the NetCDF-Java library (https://docs.unidata.ucar.edu/netcdf-java/current/userguide/). As such, this also supports reading HDF and other formats supported by NetCDF-Java.

## Data Types

The NetcdfAdapter tries to validate that the model (e.g. from fdml) matches the contents of the netCDF file. It is strict in the matching of scalar data types. Most are an obvious mapping to the netCDF data types (https://docs.unidata.ucar.edu/netcdf-java/current/javadoc/ucar/ma2/DataType.html). The unsigned integer types must be expressed using the next larger type:

    UBYTE  -> short
    USHORT -> int
    UINT   -> long
    ULONG  -> double

`ENUM`, `OPAQUE`, `SEQUENCE`, and `STRUCTURE` types are not currently supported, largely because I have seen no examples in the wild.
