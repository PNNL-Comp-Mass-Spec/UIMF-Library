==============
UIMF Library
==============

This software package includes a library of C# functions to create, 
modify and extract data from UIMF files. UIMF files are SQLite 
databases of mass spectrometry data acquired at Pacific Northwest
National Laboratory (PNNL).


How to use the UIMFLibrary in C#:
------------------------------   
    1) Copy UIMFLibrary.dll and System.Data.SQLite.DLL from 
        UIMFLibrary\bin\x86  or 
        UIMFLibrary\bin\x64  to your project
    2) Open Solution Explorer, add UIMFLibrary to your Project/References
    3) Add this using statement at the top of your code: 
          using UIMFLibrary;
    4) Declare the DataReader and DataWriter classes in your main code:

    5) Now the functions can be called as such:

    UIMFReader DataReader = new UIMFReader(filePath);
    DataReader.GetSpectrum(frameNumber, frameType, scanNumber, out mzArray, out intensityArray)
        ......

    UIMFWriter DataWriter = new UIMFWriter(filePath);
    DataWriter.CreateTables();


How to use the UIMFLibrary in C++:
-------------------------------   
    1) Copy UIMFLibrary.dll and System.Data.SQLite.DLL from 
        UIMFLibrary\bin\x86  or 
        UIMFLibrary\bin\x64  to your project
    2) Open the Solution Explorer, add UIMFLibrary to your Project/References
    3) Add this using statement at the top of your code: 
          using namespace UIMFLibrary;
    4) In your .h file, declare the DataReader and DataWriter classes: 

    gcroot<UIMFReader*> ReadUIMF;
    gcroot<UIMFWriter*> WriteUIMF;

    5) In you .cpp file, instantantiate the reader and the writer:

    6) Now the functions can be called as such:

    ReadUIMF = new UIMFReader(filePath);
    ReadUIMF->GetSpectrum(frameNumber, scanNumber, mzArray, intensityArray);
        ......

    WriteUIMF = new UIMFWriter(filePath);  
    WriteUIMF->CreateTables();


Examples:
---------
To see examples of how to create, modify, and extract data out of the database, 
refer to the UIMFLibrary_Demo solution.


UIMF database structure:
------------------------
    Four tables are included in the UIMF database file: 
      Global_Parameters
      Frame_Parameters 
      Frame_Scans
      Log_Entries


    Global_Parameters table:
    -----------------------
    Column Name             Data Type    Comment
    ----------              ----------   -------
    DateStarted             date         Date Experiment was acquired
    NumFrames               int          Number of frames in dataset
    TimeOffset              int          Offset from 0. All bin numbers must be offset by this amount
    BinWidth                double       Width of TOF bins (in ns)
    Bins                    int          Total TOF bins in a frame
    TOFCorrectionTime       float        Time Delay correction
    FrameDataBlobVersion    float        Version of FrameDataBlob in table Frame_Parameters
    ScanDataBlobVersion     float        Version of ScanInfoBlob in table Frame_Parameters
    TOFIntensityType        string       Data type of intensity in each TOF record (ADC is int/TDC is short/FOLDED is float)
    DatasetType             string       Type of dataset (HMS/HMSMS/HMS-MSn)
    Prescan_TOFPulses       int          Prescan TOP Pulses
    Prescan_Accumulations   int          Prescan Accumulations
    Prescan_TICThreshold    int          Prescan TIC Threshold
    Prescan_Continuous      bool         Prescan continous mode (True or False)
    Prescan_Profile         string       Prescan Profile File Name
    Instrument_Name         string       Instrument name


    Frame_Parameters table:
    ----------------------
    Column Name                 Data Type        Comment
    -----------                 ---------        -------
    FrameNum                    Integer          (Primary Key) Frame number
    StartTime                   DateTime         Start time of frame
    Duration                    double           Duration of frame
    Accumulations               int              Number of collected and summed acquisitions in a frame
    FrameType                   short            Bitmap: 0=MS (Legacy); 1=MS (Regular); 2=MS/MS (Frag); 3=Calibration; 4=Prescan
    Scans                       int              Number of TOF scans
    IMFProfile                  string           File name for IMF Profile
    TOFLOsses                   double           TOF Losses
    AverageTOFLength            double           Average time between TOF trigger pulses
    CalibrationSlope            double           Value of k0
    CalibrationIntercept        double           Value of t0
    a2                          double           secondary coefficients for residual mass error correction
    b2                          double           secondary coefficients for residual mass error correction
    c2                          double           secondary coefficients for residual mass error correction
    d2                          double           secondary coefficients for residual mass error correction
    e2                          double           secondary coefficients for residual mass error correction
    f2                          double           secondary coefficients for residual mass error correction
    Temperature                 double           Ambient temperature
    voltHVRack1                 double           HVRack Voltage
    voltHVRack2                 double           HVRack Voltage
    voltHVRack3                 double           HVRack Voltage
    voltHVRack4                 double           HVRack Voltage
    voltCapInlet                double           Capilary Inlet Voltage
    voltEntranceIFTIn           double           IFT In Voltage
    voltEntranceIFTOut          double           IFT Out Voltage
    voltEntranceCondLmt         double           Cond Limit Voltage
    voltTrapOut                 double           Trap Out Voltage
    voltTrapIn                  double           Trap In Voltage
    voltJetDist                 double           Jet Disruptor Voltage
    voltQuad1                   double           Fragmentation Quadrupole Voltage
    voltCond1                   double           Fragmentation Conductance Voltage
    voltQuad2                   double           Fragmentation Quadrupole Voltage
    voltCond2                   double           Fragmentation Conductance Voltage
    voltIMSOut                  double           IMS Out Voltage
    voltExitIFTIn               double           IFT In Voltage
    voltExitIFTOut              double           IFT Out Voltage
    voltExitCondLimit           double           Cond Limit Voltage
    PressureFront               double           Pressure at front of Drift Tube
    PressureBack                double           Pressure at back of Drift Tube
    MPBitOrder                  smallint         Determines original size of bit sequence
    FragmentationProfile        binary (BLOB)    Voltage profile used in fragmentation, Length number of Scans
    HighPressureFunnelPressure  double           
    IonFunnelTrapPressure       double           
    QuadropolePressure          double           
    ESIVoltage                  double           
    FloatVolage                 double           
    CALIBRATIONDONE             smallint         Set to 1 after a frame has been calibrated
    Decoded                     smallint         Set to 1 after a frame has been decoded


    Frame_Scans table:
    -----------------
    Column Name    Data Type          Comment
    -----------    ---------          -------
    FrameNum       Integer            Contains the frame number
    ScanNum        Int                Contains the TOF pulse number the spectra are located in
    NonZeroCount   Int                Nonzero intensities
    BPI            Double/Float/Int   Base Peak Intensity per Scan
    BPI_MZ         Double/Float/Int   m/z associated with BPI
    TIC            Double/Float/Int   Total Ion Chromatogram per Scan
    Intensities    binary (BLOB)      Intensities in compressed binary format 


    Log_Entries table:
    -----------------
    Column Name    Data Type   Comment
    -----------    ---------   -------
    Entry_ID       integer     Log entry ID
    Posted_By      string      Log source
    Posting_Time   string      DateTime
    Type           string      Message type
    Message        string      Log message


---------------------------------------------------------------------------------------------------------------
Written by Yan Shi for the Department of Energy (PNNL, Richland, WA)
Copyright 2009, Battelle Memorial Institute.  All Rights Reserved.

Additional contributions by Anuj Shah, Matthew Monroe, Gordon Slysz, Kevin Crowell, and Bill Danielson

E-mail: matthew.monroe@pnnl.gov or proteomics@pnl.gov
Website: http://omics.pnl.gov/software/
----------------------------------------------------------------------------------------------------------------

