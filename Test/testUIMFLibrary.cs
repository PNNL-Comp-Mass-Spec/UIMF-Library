////////////////////////////////////////////////////////////////////
// This code is written to test the UIMF reader and writer functions
// Author: Yan Shi, PNNL, December 2008
/////////////////////////////////////////////////////////////////////

//choose one of the following four options for a specific test:
//#define CREATE_UIMF
//#define UPDATE_UIMF
#define READ_UIMF
//#define ALTER_UIMF

using DATATYPE = System.Int32;

using System;
using System.Data;
using System.IO;
using System.Runtime.InteropServices;
using System.Collections;
using System.Text;

class testUIMFLibrary
{
    static void Main()
    {
        UIMFLibrary.DataReader oldData = new UIMFLibrary.DataReader();
        UIMFLibrary.DataReader newData = new UIMFLibrary.DataReader();


#if CREATE_UIMF

        string FileName = "C:\\testUIMF.db";
        if (File.Exists(FileName))
        {
            Console.WriteLine("UIMF File:" + FileName);
            File.Delete(FileName);
        }

        //Open UIMF file
        DataWriter.OpenUIMF(FileName);

        //Create Tables in UIMF file
        DataWriter.CreateTables("int");

        //Insert Global
        UIMFLibrary.GlobalParameters header = new UIMFLibrary.GlobalParameters();
        header.DateStarted = DateTime.Now;
        header.NumFrames = 10;
        header.TimeOffset = 10000;
        header.BinWidth = 0.8;
        header.Bins = 88000;
        header.FrameDataBlobVersion = 0.1F;
        header.ScanDataBlobVersion = 0.1F;
        header.TOFIntensityType = "ADC";
        header.DatasetType = "Unknown";
        DataWriter.InsertGlobal(header);

        //Insert Frame
        UIMFLibrary.FrameParameters fp = new UIMFLibrary.FrameParameters();
        fp.Duration = 0;
        fp.Accumulations = 250;
        fp.FrameType = 0;
        fp.Scans = 630;
        fp.AverageTOFLength = 123988.2F;
        fp.CalibrationSlope = 0.3333F;
        fp.CalibrationIntercept = 0.2222F;
        fp.Temperature = 0;
        fp.voltHVRack1 = 0;
        fp.voltHVRack2 = 0;
        fp.voltHVRack3 = 0;
        fp.voltHVRack4 = 0;
        fp.voltCapInlet = 0;            // 14, Capilary Inlet Voltage 
        fp.voltEntranceIFTIn = 0;       // 15, IFT In Voltage 
        fp.voltEntranceIFTOut = 0;      // 16, IFT Out Voltage 
        fp.voltEntranceCondLmt = 0;     // 17, Cond Limit Voltage
        fp.voltTrapOut = 0;             // 18, Trap Out Voltage
        fp.voltTrapIn = 0;             // 19, Trap In Voltage
        fp.voltJetDist = 0;             // 20, Jet Disruptor Voltage
        fp.voltQuad1 = 0;               // 21, Fragmentation Quadrupole Voltage
        fp.voltCond1 = 0;               // 22, Fragmentation Conductance Voltage
        fp.voltQuad2 = 0;               // 23, Fragmentation Quadrupole Voltage
        fp.voltCond2 = 0;               // 24, Fragmentation Conductance Voltage
        fp.voltIMSOut = 0;              // 25, IMS Out Voltage
        fp.voltExitIFTIn = 0;           // 26, IFT In Voltage
        fp.voltExitIFTOut = 0;          // 27, IFT Out Voltage
        fp.voltExitCondLmt = 0;         // 28, Cond Limit Voltage
        fp.PressureFront = 0;
        fp.PressureBack = 0;
        fp.MPBitOrder = 0;
        fp.FragmentationProfile = new double[1000]; 

        for (int i = 0; i < 10; i++)  //Loop through numFrames
        {
            fp.FrameNum = i + 1;
            DataWriter.InsertFrame(fp);
        }
        
        // Insert scans
        int numFrames = 10;
        int numScans = 630;
        int[] intensities = new int[98000]; // This should be the full intensity array including all zeros
        for (int i = 1; i < numFrames + 1; i++)
        {
            for (int j = 0; j < numScans; j++)
            {
                for (int k = 0; k < 98000; k++ )
                    if (k > 30000 && k < 40000) intensities[k] = 300;
                DataWriter.InsertScan(fp, j, intensities, header.BinWidth);
            }
        }
        
        //Close UIMF file
        DataWriter.CloseUIMF(FileName);
#endif

#if UPDATE_UIMF

        string FileName = "C:\\test\\testUIMF.db";
        
        if (File.Exists(FileName))
        {
            //Open UIMF file
            DataWriter.OpenUIMF(FileName);

            //Update Calibration Coefficients
            float slope = 0.575978883F;
            float intercept = 0.095746331F;

            for (int i = 0; i < 10; i++)
            {
                int frameNum = i + 1;
                DataWriter.UpdateCalibrationCoefficients(frameNum, slope, intercept);
            }
            //Close UIMF file
            DataWriter.CloseUIMF(FileName);
        }
        else
        {
            Console.WriteLine(FileName + " does not exists.");
        }

#endif


#if READ_UIMF



#if UPDATE_UIMF
		string FileName = "C:\\IMSExperiment\\HP_16\\HP_Fraction_16_0001.uimf";
		FileName = "C:\\test_data\\QC_Shew\\QC_Shew_0.25mg_4T_1.6_600_335_50ms_fr2400_adc_0000.uimf";
		FileName = "C:\\IMSExperiment\\NewData\\HP_FT1_dm_c1_600_50_0000.uimf";
		FileName = "C:\\IMSExperiment\\IMF\\35min_QC_Shew_Formic_4T_1.8_500_20_30ms_fr1950_0000.uimf";
		FileName = "C:\\IMSExperiment\\test-singleIMF\\35min_QC_Shew_Formic_4T_1.8_500_20_30ms_fr1950_0000.uimf";
		FileName = "C:\\IMSExperiment\\20090723\\35min_QC_Shew_Formic_4T_1.8_500_20_30ms_fr1950_0000.uimf";
		FileName = "C:\\IMSExperiment\\20090723\\Copy35min_QC_Shew_Formic_4T_1.8_500_20_30ms_fr1950_0000.uimf";
		FileName = "C:\\test\\agilent_600_50_adc_fr10_0000.uimf.newformat";
		FileName = "C:\\test\\agilent_600_50_adc_fr10_0000.uimf.nozero";
		FileName = "C:\\test\\agilent_600_50_adc_fr10_0000.uimf";
#endif

		string FileName1 = "C:\\IMS\\Datasets\\HP\\Test\\OldConverter.uimf";
        string FileName2 = "C:\\IMS\\Datasets\\HP\\Test\\10hr_HP_0.5mg_4T_1.8_600_335_50ms_fr2800_adc_0000.uimf";

        //string FileName = "C:\\ProteomicsSoftwareTools\\testfolder\\bsaO18_1_1_4T_1.8_500_200_30ms_0000OldConverter.uimf";
        int startScan = 186;
        int endScan = 499;
        int startBin = 0;
        int endBin = 100000;
        int startFrame = 1;
        int endFrame = 3;
        int frameType = 0;

        //Open UIMF file
        if (oldData.OpenUIMF(FileName1) == false || newData.OpenUIMF(FileName2) == false)
        {
            Console.WriteLine("Please check the file name and press return to exit");
            Console.ReadLine();
            return;
        };

		double[] mzs1 = new double[endBin - startBin + 1];
		int[] intensity1 = new int[endBin - startBin + 1];

		double[] mzs2 = new double[endBin - startBin + 1];
		int[] intensity2 = new int[endBin - startBin + 1];
		
		int oldValue = oldData.SumScans(mzs1, intensity1, frameType, startFrame, endFrame, startScan);
		int newValue = newData.SumScans(mzs2, intensity2, frameType, startFrame, endFrame, startScan);







        ///////////////////////////////////////////////////////////////////////////
        //Get Global Parameters
        ///////////////////////////////////////////////////////////////////////////

        //Extract all global parameters
        Console.WriteLine("Extract all global parameters: ");
        UIMFLibrary.GlobalParameters global_parameters = oldData.GetGlobalParameters();
        Console.WriteLine("NumFrames = " + global_parameters.NumFrames);
        Console.WriteLine("TimeOffset = " + global_parameters.TimeOffset);
        Console.WriteLine("BinWidth = " + global_parameters.BinWidth + "\n");


        //Extract a particular global parameter
        Console.WriteLine("Extract a particular global parameter: ");

        int NumFrames = global_parameters.NumFrames;
        Console.WriteLine("NumFrames = " + NumFrames + "\n");

        DateTime Start = global_parameters.DateStarted;
        Console.WriteLine("Date Started = " + Start + "\n");

        //////////////////////////////////////////////////////////////////////////
        //Get Frame Parameters
        //////////////////////////////////////////////////////////////////////////

        //Extract all frame parameters
        UIMFLibrary.FrameParameters frame_parameters = oldData.GetFrameParameters(startFrame);
        Console.WriteLine("FrameNum = " + frame_parameters.FrameNum);
        Console.WriteLine("StartTime = " + frame_parameters.StartTime);
        Console.WriteLine("Duration = " + frame_parameters.Duration);
        Console.WriteLine("CalibrationSlope = " + frame_parameters.CalibrationSlope);
        Console.WriteLine("CalibrationIntercept = " + frame_parameters.CalibrationIntercept + "\n");

        //Extract a particular frame parameter from a frame
        //Console.WriteLine("Extract a particular frame parameter from a frame");
        for (int i = 1; i <= 3; i++)
        {
            UIMFLibrary.FrameParameters fp = oldData.GetFrameParameters(i);
            double front_pressure = fp.PressureFront;
            Console.WriteLine("FrameNum = " + i + "PressureFornt = " + front_pressure + "\n");
        }

        ///////////////////////////////////////////////////////////////////////////////////////
        // Get Spectra
        ///////////////////////////////////////////////////////////////////////////////////////			
        int arrayCount = oldData.GetCountPerSpectrum(1, 167);
        Console.WriteLine("array count " + arrayCount.ToString());

        int[] spectrum = new int[arrayCount];
        double[] binsValues = new double[arrayCount];

        oldData.GetSpectrum(1, 167, spectrum, binsValues);

        for (int i = 0; i < spectrum.Length; i++)
        {
            Console.WriteLine("mz[" + i.ToString() + "]=" + binsValues[i].ToString() + ";;" + spectrum[i].ToString());
        }

        //Get TIC - Get Total Ion Chromatogram
        Console.WriteLine("Get Total Ion Chromatogram: ");
        int[] intensities = new int[(endFrame - startFrame + 1) * (endScan - startScan + 1)];
        int count = oldData.GetCountPerSpectrum(1, 2);
        count = oldData.GetCountPerSpectrum(1, 4);
        count = oldData.GetCountPerSpectrum(1, 69);
        count = oldData.GetCountPerSpectrum(1, 73);
        oldData.GetTIC(intensities, frameType, startFrame, endFrame, startScan, endScan);
        int TIC = 10;
        TIC = (int)oldData.GetTIC(1, 1);

        string Output_FileName = "test1.csv";
        FileStream outfile = new FileStream(Output_FileName, FileMode.Create, FileAccess.Write);
        StreamWriter sw = new StreamWriter(outfile);
        for (int i = 0; i < intensities.Length; i++)
        {
            sw.WriteLine(intensities[i]);
        }
        sw.Close();
        Console.WriteLine("TIC values are saved in test1.csv");

        //Sum intensities
        Console.WriteLine("Get summed intensities: ");
        double[] mzs = new double[endBin - startBin + 1];
        int[] intensity = new int[endBin - startBin + 1];
        int max_bins = oldData.SumScans(mzs, intensity, frameType, startFrame, endFrame, startScan, endScan);
        //DataReader.SumScans(mzs, intensity, frameType, 1,2,225,225);

        if (max_bins > 0)
        {
            string mz_int_FileName = "C:\\sum_intensity.csv";
            FileStream mz_int_outfile = new FileStream(mz_int_FileName, FileMode.Create, FileAccess.Write);
            StreamWriter sw_mz_int = new StreamWriter(mz_int_outfile);
            for (int i = 0; i < intensity.Length; i++)
            {
                if (intensity[i] > 0)
                {
                    sw_mz_int.WriteLine(mzs[i] + ", " + intensity[i]);
                }
            }
            sw_mz_int.Close();
            Console.WriteLine("summed intensities are saved in sum_intensity.csv");
        }
        else
        {
            Console.WriteLine("All intensities are zero from frame " + startFrame + " to " + endFrame + " and scan " + startScan + " to " + endScan);
        }
        //Get Spectrum
        Console.WriteLine("Get Spectrum: ");
        int frameNum = 1;
        int scanNum = 6;
        DATATYPE[] intensity_spec = new DATATYPE[endBin - startBin + 1];
        double[] mzs_spec = new double[endBin - startBin + 1];

        int out_len = oldData.GetSpectrum(frameNum, scanNum, intensity_spec, mzs_spec);
        if (out_len > 0)
        {
            string outFileName = "C:\\UIMFRead_TestResults\\spectrum1.csv";
            FileStream specoutfile = new FileStream(outFileName, FileMode.Create, FileAccess.Write);
            StreamWriter spec_mz_int = new StreamWriter(specoutfile);
            for (int i = 0; i < out_len; i++)
            {
                spec_mz_int.WriteLine(mzs_spec[i] + "," + intensity_spec[i]);
            }
            spec_mz_int.Close();
            Console.WriteLine("Spectrum values are saved in spectrum1.csv");
        }
        else
        {
            Console.WriteLine("All intensities are zero for frame " + frameNum + " and scan " + scanNum);
        }
        //Close UIMF file
        oldData.CloseUIMF();

        Console.WriteLine("\nTesting successful, click return to end this process");
        Console.ReadLine();

#endif

#if ALTER_UIMF

		string FileName = "C:\\test\\testUIMF.db";
		DataWriter.OpenUIMF(FileName);

		//DataWriter.AddGlobalParameter("TOFCorrectionTime", "FLOAT");
		//DataWriter.UpdateGlobalParameter("TOFCorrectionTime","29.6");

        /*
		DataWriter.AddFrameParameter("FrameTest", "INT");
        DataWriter.UpdateGlobalParameter("NumFrames", "10");
		

		DataWriter.AddFrameParameter("a2", "DOUBLE");
		DataWriter.AddFrameParameter("b2", "DOUBLE");
		DataWriter.AddFrameParameter("c2", "DOUBLE");
		DataWriter.AddFrameParameter("d2", "DOUBLE");
		DataWriter.AddFrameParameter("e2", "DOUBLE");
		DataWriter.AddFrameParameter("f2", "DOUBLE");
        */
		
		for (int i = 1; i <= 10; i++)
		{
			int FrameNum = i;
			DataWriter.UpdateFrameParameter(FrameNum, "a2", "0.2");
			DataWriter.UpdateFrameParameter(FrameNum, "b2", "0.3");
			DataWriter.UpdateFrameParameter(FrameNum, "c2", "0.4");
			DataWriter.UpdateFrameParameter(FrameNum, "d2", "0.1");
			DataWriter.UpdateFrameParameter(FrameNum, "e2", "0.6");
			DataWriter.UpdateFrameParameter(FrameNum, "f2", "0.07");
		}
		
		DataWriter.CloseUIMF(FileName);

#endif
    }
}

