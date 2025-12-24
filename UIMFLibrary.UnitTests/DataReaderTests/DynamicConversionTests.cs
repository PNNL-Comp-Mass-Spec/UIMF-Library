using System;
using NUnit.Framework;
using UIMFLibrary.UnitTests.Legacy;

namespace UIMFLibrary.UnitTests.DataReaderTests
{
    [TestFixture]
    public class DynamicConversionTests
    {
        [Test]
        [TestCase("Test data", typeof(string), true)]
        [TestCase("3", typeof(byte), true)]
        [TestCase("3", typeof(short), true)]
        [TestCase("3", typeof(int), true)]
        [TestCase("3", typeof(double), true)]
        [TestCase("3", typeof(float), true)]
        [TestCase("3.0", typeof(byte), true)]
        [TestCase("3.0", typeof(short), true)]
        [TestCase("3.0", typeof(int), true)]
        [TestCase("3.0", typeof(double), true)]
        [TestCase("3.0", typeof(float), true)]
        [TestCase("3.5", typeof(byte), false)]
        [TestCase("3.5", typeof(short), false)]
        [TestCase("3.5", typeof(int), false)]
        [TestCase("3.5", typeof(double), true)]
        [TestCase("3.5", typeof(float), true)]
        [TestCase("not a number", typeof(byte), false)]
        [TestCase("not a number", typeof(short), false)]
        [TestCase("not a number", typeof(int), false)]
        [TestCase("not a number", typeof(double), false)]
        [TestCase("not a number", typeof(float), false)]
        public void TestConvertStringToDynamic(string value, Type targetType, bool convertible)
        {
            var result = FrameParamUtilities.ConvertStringToDynamic(targetType, value);

            if (result == null)
            {
                // Conversion failed
                if (convertible)
                    Assert.Fail($"Conversion of {value} to {targetType} should have succeeded");

                Console.WriteLine("As expected, could not convert {0} to {1}", value, targetType);
                return;
            }

            if (targetType == typeof(string))
            {
                Assert.That((string)result, Is.EqualTo(value));
                return;
            }

            Console.WriteLine("Converted {0} to {1}", value, targetType);
        }

        [Test]
        [TestCase("3", true)]
        [TestCase("3.234", true)]
        [TestCase("-3.234", true)]
        [TestCase("53E3", true)]
        [TestCase("53E+3", true)]
        [TestCase("not a number", false)]
        public void TestConvertDynamicToDouble(string valueText, bool convertible)
        {
            dynamic valueAsDynamic = valueText;

            var success = FrameParamUtilities.ConvertDynamicToDouble(valueAsDynamic, out double value);

            if (success)
            {
                Console.WriteLine("Converted {0} to {1}", valueText, value);
            }

            if (convertible)
            {
                Assert.That(success, Is.True, $"Conversion to double failed for {valueText}");
            }
            else
            {
                Assert.That(success, Is.False, $"Conversion to double unexpectedly succeeded for {valueText}");

                Console.WriteLine("As expected, could not convert {0} to a double", valueText);
            }
        }

        [Test]
        [TestCase("3", true)]
        [TestCase("3.234", false)]
        [TestCase("-3.234", false)]
        [TestCase("6983224", true)]
        [TestCase("53E3", false)]
        [TestCase("53E+3", false)]
        [TestCase("not a number", false)]
        public void TestConvertDynamicToInt32(string valueText, bool convertible)
        {
            dynamic valueAsDynamic = valueText;

            var success = FrameParamUtilities.ConvertDynamicToInt32(valueAsDynamic, out int value);

            if (success)
            {
                Console.WriteLine("Converted {0} to {1}", valueText, value);
            }

            if (convertible)
            {
                Assert.That(success, Is.True, $"Conversion to int failed for {valueText}");
            }
            else
            {
                Assert.That(success, Is.False, $"Conversion to int unexpectedly succeeded for {valueText}");

                Console.WriteLine("As expected, could not convert {0} to a int", valueText);
            }
        }

        [Test]
        public void TestConvertFrameParametersToDictionary()
        {
            var frameParameters = GetExampleFrameParameters();
#pragma warning disable CS0618 // Type or member is obsolete
            var frameParams = FrameParameters.ConvertFrameParameters(frameParameters);
#pragma warning restore CS0618 // Type or member is obsolete

            var success = frameParams.TryGetValue(FrameParamKeyType.StartTimeMinutes, out var startTime);
            Assert.That(success, Is.True, "Start time could not be retrieved");
            Assert.That(startTime, Is.EqualTo(frameParameters.StartTime).Within(0.00001));

            success = frameParams.TryGetValue(FrameParamKeyType.Scans, out var scans);
            Assert.That(success, Is.True, "Scans could not be retrieved");
            Assert.That(scans, Is.EqualTo(frameParameters.Scans));

            success = frameParams.TryGetValue(FrameParamKeyType.CalibrationSlope, out var calibrationSlope);
            Assert.That(success, Is.True, "CalibrationSlope could not be retrieved");
            Assert.That(calibrationSlope, Is.EqualTo(frameParameters.CalibrationSlope).Within(0.00001));

            success = frameParams.TryGetValue(FrameParamKeyType.FrameType, out var frameType);
            Assert.That(success, Is.True, "FrameType could not be retrieved");
            Assert.That(frameType, Is.EqualTo((int)frameParameters.FrameType));
        }

        [Test]
        public void TestConvertFrameParametersToClass()
        {
            var frameParameters = GetExampleFrameParameters();
#pragma warning disable CS0618 // Type or member is obsolete
            var frameParamsByType = FrameParameters.ConvertFrameParameters(frameParameters);
#pragma warning restore CS0618 // Type or member is obsolete
            var frameParams = new FrameParams(frameParameters.FrameNum, frameParamsByType);

            var startTime = frameParams.GetValueDouble(FrameParamKeyType.StartTimeMinutes);
            Assert.That(startTime, Is.EqualTo(frameParameters.StartTime).Within(0.00001));

            var startTimeDynamicInt = frameParams.GetValue(FrameParamKeyType.StartTimeMinutes, 0);
            Assert.That(startTimeDynamicInt, Is.EqualTo(frameParameters.StartTime).Within(0.00001));

            var startTimeDynamicDouble = frameParams.GetValue(FrameParamKeyType.StartTimeMinutes, 0.0);
            Assert.That(startTimeDynamicDouble, Is.EqualTo(frameParameters.StartTime).Within(0.00001));

            var startTimeDynamic= frameParams.GetValue(FrameParamKeyType.StartTimeMinutes);
            Assert.That(startTimeDynamic, Is.EqualTo(frameParameters.StartTime).Within(0.00001));

            var scans = frameParams.GetValueInt32(FrameParamKeyType.Scans);
            Assert.That(scans, Is.EqualTo(frameParameters.Scans));

            var scansDynamicInt = frameParams.GetValue(FrameParamKeyType.Scans, 0);
            Assert.That(scansDynamicInt, Is.EqualTo(frameParameters.Scans));

            var scansDynamicDouble = frameParams.GetValue(FrameParamKeyType.Scans, 0.0);
            Assert.That(scansDynamicDouble, Is.EqualTo(frameParameters.Scans).Within(0.00001));

            var calibrationSlope = frameParams.GetValueDouble(FrameParamKeyType.CalibrationSlope);
            Assert.That(calibrationSlope, Is.EqualTo(frameParameters.CalibrationSlope).Within(0.00001));

            var calibrationSlopeDynamicInt = frameParams.GetValue(FrameParamKeyType.CalibrationSlope, 0);
            Assert.That(calibrationSlopeDynamicInt, Is.EqualTo(frameParameters.CalibrationSlope));

            var calibrationSlopeDynamicDouble = frameParams.GetValue(FrameParamKeyType.CalibrationSlope, 0.0);
            Assert.That(calibrationSlopeDynamicDouble, Is.EqualTo(frameParameters.CalibrationSlope).Within(0.00001));

            // Remove the slope and try again
            frameParams.Values.Remove(FrameParamKeyType.CalibrationSlope);
            calibrationSlope = frameParams.GetValueDouble(FrameParamKeyType.CalibrationSlope, 3.4);
            Assert.That(calibrationSlope, Is.EqualTo(3.4).Within(0.00001));

            calibrationSlopeDynamicInt = frameParams.GetValue(FrameParamKeyType.CalibrationSlope, 2);
            Assert.That(calibrationSlopeDynamicInt, Is.EqualTo(2));

            calibrationSlopeDynamicDouble = frameParams.GetValue(FrameParamKeyType.CalibrationSlope, 2.5);
            Assert.That(calibrationSlopeDynamicDouble, Is.EqualTo(2.5).Within(0.00001));

            var frameType = frameParams.GetValueInt32(FrameParamKeyType.FrameType);
            Assert.That(frameType, Is.EqualTo((int)frameParameters.FrameType));

            var frameTypeDynamic = frameParams.GetValue(FrameParamKeyType.FrameType, 0);
            Assert.That(frameTypeDynamic, Is.EqualTo((int)frameParameters.FrameType));

            var frameTypeDynamicDouble = frameParams.GetValue(FrameParamKeyType.FrameType, 0.0);
            Assert.That(frameTypeDynamicDouble, Is.EqualTo((double)frameParameters.FrameType).Within(0.00001));

            var imfProfile = frameParams.GetValue(FrameParamKeyType.MultiplexingEncodingSequence);
            Assert.That(imfProfile, Is.EqualTo("Seq"));

            var imfProfileWithDefault = frameParams.GetValue(FrameParamKeyType.MultiplexingEncodingSequence, "");
            Assert.That(imfProfileWithDefault, Is.EqualTo("Seq"));

            var imfProfileString = frameParams.GetValueString(FrameParamKeyType.MultiplexingEncodingSequence);
            Assert.That(imfProfileString, Is.EqualTo("Seq"));
        }

        [Test]
        public void TestConvertGlobalParametersToDictionary()
        {
            var globalParameters = GetExampleGlobalParameters();
#pragma warning disable CS0618 // Type or member is obsolete
            var globalParams = GlobalParameters.ConvertGlobalParameters(globalParameters);
#pragma warning restore CS0618 // Type or member is obsolete

            var success = globalParams.TryGetValue(GlobalParamKeyType.InstrumentName, out var instName);
            Assert.That(success, Is.True, "Instrument name could not be retrieved");
            Assert.That(instName, Is.EqualTo(globalParameters.InstrumentName));

            success = globalParams.TryGetValue(GlobalParamKeyType.NumFrames, out var numFrames);
            Assert.That(success, Is.True, "NumFrames could not be retrieved");
            Assert.That(numFrames, Is.EqualTo(globalParameters.NumFrames));

            success = globalParams.TryGetValue(GlobalParamKeyType.BinWidth, out var binWidth);
            Assert.That(success, Is.True, "BinWidth could not be retrieved");
            Assert.That(binWidth, Is.EqualTo(globalParameters.BinWidth).Within(0.00001));
        }

        [Test]
        public void TestConvertGlobalParametersToClass()
        {
#pragma warning disable CS0618 // Type or member is obsolete
            var globalParameters = GetExampleGlobalParameters();
            var globalParamsByType = GlobalParameters.ConvertGlobalParameters(globalParameters);
            var globalParams = GlobalParameters.ConvertDynamicParamsToGlobalParams(globalParamsByType);
#pragma warning restore CS0618 // Type or member is obsolete

            var instName = globalParams.GetValueString(GlobalParamKeyType.InstrumentName);
            Assert.That(instName, Is.EqualTo(globalParameters.InstrumentName));

            var instNameDynamic = globalParams.GetValue(GlobalParamKeyType.InstrumentName, "");
            Assert.That(instNameDynamic, Is.EqualTo(globalParameters.InstrumentName));

            var instNameDynamicWithDefault = globalParams.GetValue(GlobalParamKeyType.InstrumentName);
            Assert.That(instNameDynamicWithDefault, Is.EqualTo(globalParameters.InstrumentName));

            // Remove the instrument name then test again
            globalParams.Values.Remove(GlobalParamKeyType.InstrumentName);

            instName = globalParams.GetValueString(GlobalParamKeyType.InstrumentName);
            Assert.That(instName, Is.Empty);

            instName = globalParams.GetValueString(GlobalParamKeyType.InstrumentName, "Undefined");
            Assert.That(instName, Is.EqualTo("Undefined"));

            instNameDynamic = globalParams.GetValue(GlobalParamKeyType.InstrumentName, "Undefined");
            Assert.That(instNameDynamic, Is.EqualTo("Undefined"));

            instNameDynamicWithDefault = globalParams.GetValue(GlobalParamKeyType.InstrumentName);
            Assert.That(instNameDynamicWithDefault, Is.EqualTo(""));

            var numFrames = globalParams.GetValueInt32(GlobalParamKeyType.NumFrames);
            Assert.That(numFrames, Is.EqualTo(globalParameters.NumFrames));

            var numFramesDynamic = globalParams.GetValue(GlobalParamKeyType.NumFrames, 0);
            Assert.That(numFramesDynamic, Is.EqualTo(globalParameters.NumFrames));

            var numFramesDynamicDouble = globalParams.GetValue(GlobalParamKeyType.NumFrames, 0.0);
            Assert.That(numFramesDynamicDouble, Is.EqualTo(globalParameters.NumFrames).Within(0.00001));

            var binWidth = globalParams.GetValueInt32(GlobalParamKeyType.BinWidth);
            Assert.That(binWidth, Is.EqualTo(globalParameters.BinWidth));

            var binWidthDynamic = globalParams.GetValue(GlobalParamKeyType.BinWidth, 0);
            Assert.That(binWidthDynamic, Is.EqualTo(globalParameters.BinWidth));

            var binWidthDynamicDouble = globalParams.GetValue(GlobalParamKeyType.BinWidth, 0.0);
            Assert.That(binWidthDynamicDouble, Is.EqualTo(globalParameters.BinWidth).Within(0.00001));
        }

#pragma warning disable 612, 618

        private FrameParameters GetExampleFrameParameters()
        {
            return new FrameParameters
            {
                FrameNum = 2,
                StartTime = 266.95178,
                Duration = 0,
                Accumulations = 18,
                FrameType = UIMFData.FrameType.MS1,
                Scans = 360,
                IMFProfile = "Seq",
                TOFLosses = 61,
                AverageTOFLength = 162555.56,
                CalibrationSlope = 0.347341992892325,
                CalibrationIntercept = 0.0279005798339844,
                a2 = 0,
                b2 = 0,
                c2 = 0,
                d2 = 0,
                e2 = 0,
                f2 = 0,
                Temperature = 135.0,
                voltHVRack1 = 26.9,
                voltHVRack2 = 3822.0,
                voltHVRack3 = 1796.0,
                PressureFront = 3.734,
                PressureBack = 3.796,
                MPBitOrder = 0
            };
        }

        private GlobalParameters GetExampleGlobalParameters()
        {
            return new GlobalParameters
            {
                InstrumentName = "Test",
                DateStarted = "08/09/2017 8:00:00 am",
                NumFrames = 10,
                TimeOffset = 10000,
                BinWidth = 1.0,
                Bins = 138000,
                TOFCorrectionTime = 0.0f,
                FrameDataBlobVersion = 0.1f,      // Legacy parameter that was always 0.1
                ScanDataBlobVersion = 0.1f,       // Legacy parameter that was always 0.1
                TOFIntensityType = "ADC",
                DatasetType = "",
                Prescan_TOFPulses = 50,
                Prescan_Accumulations = 20,
                Prescan_TICThreshold = 300,
                Prescan_Continuous = false,
                Prescan_Profile = "SA_4ms.txt"
            };
        }

#pragma warning restore 612, 618

    }
}
