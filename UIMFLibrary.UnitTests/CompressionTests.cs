using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;

namespace UIMFLibrary.UnitTests
{
    [TestFixture]
    public class CompressionTests
    {
        private const int BinCount = 148000;
        private readonly List<KeyValuePair<int, int>> testValues = new List<KeyValuePair<int, int>>() { new KeyValuePair<int, int>(49693, 8) };
        private int[] testInputData;
        private short[] testInputDataShort;
        private readonly byte[] compressedOldImsData = new byte[] { 0x04, 0x00, 0x80, 0xFF, 0xFF, 0x00, 0x20, 0x00, 0x05, 0xE3, 0xBD, 0xFF, 0xFF, 0x08, 0x00, 0x20, 0x00, 0xA0, 0x0F, 0xE0, 0x05, 0x07, 0x01, 0x00, 0x00 };
        private readonly byte[] compressedOldImsDataShort = new byte[] { 0x08, 0x00, 0x80, 0x00, 0x00, 0xE3, 0xBD, 0x08, 0x00, 0x00, 0x20, 0x07, 0x80, 0x03, 0x01, 0x00, 0x00 };
        private readonly byte[] compressedCurrentData = new byte[] { 0x07, 0xE3, 0x3D, 0xFF, 0xFF, 0x08, 0x00, 0x00, 0x00 };
        private readonly byte[] compressedCurrentDataShort = new byte[] { 0x0B, 0x00, 0x80, 0xE3, 0xBD, 0x08, 0x00, 0x00, 0x80, 0x00, 0x80, 0x00, 0x80 };

        [OneTimeSetUp]
        public void CreateTestData()
        {
            testInputData = new int[BinCount];
            testInputDataShort = new short[BinCount];
            foreach (var value in testValues)
            {
                testInputData[value.Key] = value.Value;
                testInputDataShort[value.Key] = (short)value.Value;
            }
        }

        [Test]
        public void CompressionRoundTripTest1()
        {
            var encoded = Compress(testInputData);
            var decoded = Decompress(encoded, BinCount, out int _);

            for (var i = 0; i < BinCount; i++)
            {
                Assert.AreEqual(testInputData[i], decoded[i], 0, "Mismatch at bin {0}", i);
            }
        }

        [Test]
        public void CompressionRoundTripTest2()
        {
            var decoded = Decompress(compressedCurrentData, BinCount, out int _);
            var encoded = Compress(decoded);

            Assert.AreEqual(compressedCurrentData.Length, encoded.Length);

            for (var i = 0; i < encoded.Length; i++)
            {
                Assert.AreEqual(compressedCurrentData[i], encoded[i], 0, "Mismatch at index {0}", i);
            }
        }

        [Test]
        public void CompressTest()
        {
            var encoded = Compress(testInputData);
            Assert.AreEqual(compressedCurrentData.Length, encoded.Length);
            for (var i = 0; i < compressedCurrentData.Length; i++)
            {
                Assert.AreEqual(compressedCurrentData[i], encoded[i], 0, "Mismatch at index {0}", i);
            }
        }

        [Test]
        public void DecompressOldImsDataTest()
        {
            var decoded = Decompress(compressedOldImsData, BinCount, out int _);

            for (var i = 0; i < BinCount; i++)
            {
                Assert.AreEqual(testInputData[i], decoded[i], 0, "Mismatch at bin {0}", i);
            }
        }

        [Test]
        public void DecompressOldImsDataTest2()
        {
            var decoded = Decompress(compressedOldImsDataShort, BinCount, out short _);

            for (var i = 0; i < BinCount; i++)
            {
                Assert.AreEqual(testInputData[i], decoded[i], 0, "Mismatch at bin {0}", i);
            }
        }

        [Test]
        public void CompressShortTest()
        {
            var encoded = Compress(testInputDataShort);
            foreach (var b in encoded)
            {
                Console.WriteLine("{0:X}", b);
            }
            Assert.AreEqual(compressedCurrentDataShort.Length, encoded.Length);
            for (var i = 0; i < compressedCurrentDataShort.Length; i++)
            {
                Assert.AreEqual(compressedCurrentDataShort[i], encoded[i], 0, "Mismatch at index {0}", i);
            }
        }

        private static int[] Decompress(byte[] compressed, int binCount, out int nonZero)
        {
            var binIntensities = UIMFLibrary.IntensityConverterCLZF.Decompress(compressed, out nonZero);
            var intensities = new int[binCount];

            foreach (var binIntensity in binIntensities)
            {
                if (binIntensity.Item1 < binCount)
                {
                    intensities[binIntensity.Item1] = binIntensity.Item2;
                }
                else
                {
                    Console.WriteLine("Warning: index out of bounds in RlzDecode: {0} > {1} ", binIntensity.Item1, binCount);
                    break;
                }
            }

            return intensities;
        }

        private static short[] Decompress(byte[] compressed, int binCount, out short nonZero)
        {
            var binIntensities = UIMFLibrary.IntensityConverterCLZF.Decompress(compressed, out nonZero);
            var intensities = new short[binCount];

            foreach (var binIntensity in binIntensities)
            {
                if (binIntensity.Item1 < binCount)
                {
                    intensities[binIntensity.Item1] = binIntensity.Item2;
                }
                else
                {
                    Console.WriteLine("Warning: index out of bounds in RlzDecode: {0} > {1} ", binIntensity.Item1, binCount);
                    break;
                }
            }

            return intensities;
        }

        private static byte[] Compress(IReadOnlyList<int> intensities)
        {
            UIMFLibrary.IntensityConverterCLZF.Compress(intensities, out var compressed, out _, out _, out _);
            return compressed;
        }

        private static byte[] Compress(IReadOnlyList<short> intensities)
        {
            UIMFLibrary.IntensityConverterCLZF.Compress(intensities, out var compressed, out _, out _, out _);
            return compressed;
        }
    }
}
