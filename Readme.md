# UIMF Library

This software package includes a library of C# functions to create, 
modify and extract data from UIMF files. UIMF files are SQLite 
databases of mass spectrometry data acquired at Pacific Northwest
National Laboratory (PNNL).

### NuGet

The UIMFLibrary is available on NuGet at:
* https://www.nuget.org/packages/UIMFLibrary/

### Continuous Integration

The latest version of the application may be available on the [AppVeyor CI server](https://ci.appveyor.com/project/PNNLCompMassSpec/uimf-library/build/artifacts), 
but builds are deleted after 6 months. \
[![Build status](https://ci.appveyor.com/api/projects/status/fe9k281a3r5s1ec3?svg=true)](https://ci.appveyor.com/project/PNNLCompMassSpec/uimf-library)

## How to use the UIMFLibrary in C#:

* Use the NuGet package manager to add a reference to the UIMFLibrary
  * Note that it depends on System.Data.SQLite
```csharp
using UIMFLibrary;
```
          
* Declare the DataReader and DataWriter classes in your main code:
* Now the functions can be called as such:

```csharp
UIMFReader DataReader = new UIMFReader(filePath);
DataReader.GetSpectrum(frameNumber, DataReader.FrameType.MS1, scanNumber, out var mzArray, out var intensityArray)
    ......

UIMFWriter DataWriter = new UIMFWriter(filePath);
DataWriter.CreateTables();
```

## How to use the UIMFLibrary in C++:

* Copy UIMFLibrary.dll and System.Data.SQLite.DLL from either
  * UIMFLibrary\bin\x86 
  * UIMFLibrary\bin\x64
* Open the Solution Explorer, add UIMFLibrary to your Project/References
* Add this using statement at the top of your code: 
```c
using namespace UIMFLibrary;
```
* In your .h file, declare the DataReader and DataWriter classes: 
```c
gcroot<UIMFReader*> ReadUIMF;
gcroot<UIMFWriter*> WriteUIMF;
```
* In you .cpp file, instantantiate the reader and the writer:

* Now the functions can be called as such:
```c
ReadUIMF = new UIMFReader(filePath);
ReadUIMF->GetSpectrum(frameNumber, scanNumber, mzArray, intensityArray);
  ......

WriteUIMF = new UIMFWriter(filePath);  
WriteUIMF->CreateTables();
```

## Examples

To see examples of how to create, modify, and extract data out of the database, 
refer to the UIMFLibrary_Demo solution (see  [GitHub](https://github.com/PNNL-Comp-Mass-Spec/UIMF-Library/tree/master/UIMFLibrary_Demo))


## UIMF database structure:

Tables in a UIMF database file: 

 | Table Name | Description | Comments |
 | ----------- | --------- | ------- |
 | Global_Params | Global parameters | Parameters that apply to the entire UIMF file, with one row per parameter |
 | Frame_Param_Keys | Description of the frame parameters in Frame_Params | Each frame parameter has a unique parameter id |
 | Frame_Params | Parameters that apply to each frame | Each frame will have a series of parameters listed, with one parameter per row |
 | Frame_Scans | Description of each mass spectrum, including BPI and TIC plus the ion intensities | Intensities are stored as binary blobs |
 | Log_Entries | Log messages | Optional table |
 | Bin_Intensities | Selected ion chromatograms for each m/z bin | Optional Table tracking the intensity at a given m/z in every scan of every frame |
 | Global_Parameters | Legacy global parameters table | Wide table with a single row |
 | Frame_Parameters | Legacy frame parameters table | Wide table with one row per frame |

The UIMFWriter can optionally populate the legacy tables, but their use was phased out starting in 2015.

### Global_Params

 | Column Name | Data Type | Comment |
 | ----------- | --------- | ------- |
 | ParamID | int | Global parameter ID |
 | ParamName | string | Parameter name |
 | ParamValue | int | Parameter value |
 | ParamDataType | string | Data type for the parameter|
 | ParamDescription | string | Description of the parameter|

### Frame_Param_Keys

 | Column Name | Data Type | Comment |
 | ----------- | --------- | ------- |
 | ParamID | int | Frame parameter ID |
 | ParamName | string | Parameter name |
 | ParamDataType | string | Data type for the parameter|
 | ParamDescription | string | Description of the parameter|

### Frame_Params

 | Column Name | Data Type | Comment |
 | ----------- | --------- | ------- |
 | FrameNum | int | Frame number |
 | ParamID | int | Frame parameter ID |
 | ParamValue | int | Parameter value |

### Frame_Scans table

 | Column Name | Data Type | Comment |
 | ----------- | --------- | ------- |
 | FrameNum | int | Contains the frame number |
 | ScanNum | short | Contains the TOF pulse number the spectra are located in |
 | NonZeroCount | int | Nonzero intensities |
 | BPI | double/float/int | Base Peak Intensity per Scan |
 | BPI_MZ | double | m/z associated with BPI |
 | TIC | double/float/int | Total Ion Chromatogram per Scan |
 | Intensities | binary (BLOB) | Intensities in compressed binary format  |


### Log_Entries table

 | Column Name | Data Type | Comment |
 | ----------- | --------- | ------- |
 | Entry_ID | int | Log entry ID |
 | Posted_By | string | Log source |
 | Posting_Time | string | DateTime |
 | Type | string | Message type |
 | Message | string | Log message |

### Bin_Intensities table

 | Column Name | Data Type | Comment |
 | ----------- | --------- | ------- |
 | MZ_BIN      | integer | Bin number. Convert to m/z using ConvertBinToMZ in UIMFDataReader |
 | INTENSITIES | binary (BLOB) | Intensities for the given m/z across scans (for all frames) |

### Legacy Global_Parameters table

| Column Name | Data Type | Comment |
| ---------- | ---------- | ------- |
| DateStarted | string | Date Experiment was acquired |
| NumFrames | int | Number of frames in dataset |
| TimeOffset | int | Offset from 0. All bin numbers must be offset by this amount |
| BinWidth | double | Width of TOF bins (in ns) |
| Bins | int | Total TOF bins in a frame |
| TOFCorrectionTime | float | Time Delay correction |
| FrameDataBlobVersion | float | Version of FrameDataBlob in table Frame_Parameters |
| ScanDataBlobVersion | float | Version of ScanInfoBlob in table Frame_Parameters |
| TOFIntensityType | string | Data type of intensity in each TOF record (ADC is int/TDC is short/FOLDED is float) |
| DatasetType | string | Type of dataset (HMS/HMSMS/HMS-MSn) |
| Prescan_TOFPulses | int | Prescan TOP Pulses |
| Prescan_Accumulations | int | Prescan Accumulations |
| Prescan_TICThreshold | int | Prescan TIC Threshold |
| Prescan_Continuous | bool | Prescan continous mode (True or False) |
| Prescan_Profile | string | Prescan Profile File Name |
| Instrument_Name | string | Instrument name |


### Legacy Frame_Parameters table

 | Column Name | Data Type | Comment |
 | ----------- | --------- | ------- |
 | FrameNum | int | (Primary Key) Frame number |
 | StartTime | double | Start time of frame |
 | Duration | double | Duration of frame |
 | Accumulations | short | Number of collected and summed acquisitions in a frame |
 | FrameType | short | Bitmap: 0=MS (Legacy); 1=MS (Regular); 2=MS/MS (Frag); 3=Calibration; 4=Prescan |
 | Scans | int | Number of TOF scans |
 | IMFProfile | string | File name for IMF Profile |
 | TOFLOsses | double | TOF Losses |
 | AverageTOFLength | double | Average time between TOF trigger pulses |
 | CalibrationSlope | double | Value of k0 |
 | CalibrationIntercept | double | Value of t0 |
 | a2 | double | secondary coefficients for residual mass error correction |
 | b2 | double | secondary coefficients for residual mass error correction |
 | c2 | double | secondary coefficients for residual mass error correction |
 | d2 | double | secondary coefficients for residual mass error correction |
 | e2 | double | secondary coefficients for residual mass error correction |
 | f2 | double | secondary coefficients for residual mass error correction |
 | Temperature | double | Ambient temperature |
 | voltHVRack1 | double | HVRack Voltage |
 | voltHVRack2 | double | HVRack Voltage |
 | voltHVRack3 | double | HVRack Voltage |
 | voltHVRack4 | double | HVRack Voltage |
 | voltCapInlet | double | Capilary Inlet Voltage |
 | voltEntranceIFTIn | double | IFT In Voltage |
 | voltEntranceIFTOut | double | IFT Out Voltage |
 | voltEntranceCondLmt | double | Cond Limit Voltage |
 | voltTrapOut | double | Trap Out Voltage |
 | voltTrapIn | double | Trap In Voltage |
 | voltJetDist | double | Jet Disruptor Voltage |
 | voltQuad1 | double | Fragmentation Quadrupole Voltage |
 | voltCond1 | double | Fragmentation Conductance Voltage |
 | voltQuad2 | double | Fragmentation Quadrupole Voltage |
 | voltCond2 | double | Fragmentation Conductance Voltage |
 | voltIMSOut | double | IMS Out Voltage |
 | voltExitIFTIn | double | IFT In Voltage |
 | voltExitIFTOut | double | IFT Out Voltage |
 | voltExitCondLimit | double | Cond Limit Voltage |
 | PressureFront | double | Pressure at front of Drift Tube |
 | PressureBack | double | Pressure at back of Drift Tube |
 | MPBitOrder | short | Determines original size of bit sequence |
 | FragmentationProfile | binary (BLOB) | Voltage profile used in fragmentation, Length number of Scans |
 | HighPressureFunnelPressure  double | High pressure funnel pressure |
 | IonFunnelTrapPressure | double | Ion funnel trap pressure |
 | QuadrupolePressure | double | Quadrupole pressure |
 | ESIVoltage | double | Electrospray voltage |
 | FloatVolage | double | Float voltage |
 | CALIBRATIONDONE | int | Set to 1 after a frame has been calibrated |
 | Decoded | int | Set to 1 after a frame has been decoded |

## Contacts

Written by PNNL Staff for the Department of Energy (PNNL, Richland, WA) \
Copyright 2009, Battelle Memorial Institute.  All Rights Reserved.

Contributors include: \
Kevin Crowell, Bill Danielson, Bryson Gibbons, Matthew Monroe, Spencer Prost, Yan Shi, Anuj Shah, Gordon Slysz

E-mail: matthew.monroe@pnnl.gov or proteomics@pnl.gov \
Website: https://github.com/PNNL-Comp-Mass-Spec/UIMF-Library

## License

Licensed under the Educational Community License, Version 2.0 (the "License"); 
you may not use this program except in compliance with the License. 
You may obtain a copy of the License at

https://opensource.org/licenses/ECL-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an "AS IS"
BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
or implied. See the License for the specific language governing
permissions and limitations under the License.
