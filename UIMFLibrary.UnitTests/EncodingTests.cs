using System;
using System.Collections.Generic;
using NUnit.Framework;

namespace UIMFLibrary.UnitTests
{
    [TestFixture]
    public class EncodingTests
    {
        // Ignore Spelling: ims, rlz

        private const int BinCount = 148000;
        private readonly List<KeyValuePair<int, int>> testValues = new List<KeyValuePair<int, int>> {new KeyValuePair<int, int>(49693, 8)};
        private int[] testInputData;
        private short[] testInputDataShort;
        private readonly int[] rlzEncodedOldImsData = { -32768, 0, -16925, 8, -32768, 0, -32768, 0, -32768, 0 };
        private readonly short[] rlzEncodedOldImsDataShort = { -32768, 0, -16925, 8, -32768, 0, -32768, 0, -32768, 0 };
        private readonly int[] rlzEncodedCurrentData = { -49693, 8 };
        private readonly short[] rlzEncodedCurrentDataShort = { -32768, -16925, 8, -32768, -32768, -32768 };

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
        public void RlzEncodeRoundTripTest1()
        {
            var encoded = RlzEncode(testInputData);
            var decoded = RlzDecode(encoded, BinCount);

            for (var i = 0; i < BinCount; i++)
            {
                Assert.That(decoded[i], Is.EqualTo(testInputData[i]).Within(0), $"Mismatch at bin {i}");
            }
        }

        [Test]
        public void RlzEncodeRoundTripTest2()
        {
            var decoded = RlzDecode(rlzEncodedCurrentData, BinCount);
            var encoded = RlzEncode(decoded);

            Assert.That(encoded, Has.Length.EqualTo(rlzEncodedCurrentData.Length));

            for (var i = 0; i < encoded.Length; i++)
            {
                Assert.That(encoded[i], Is.EqualTo(rlzEncodedCurrentData[i]).Within(0), $"Mismatch at index {i}");
            }
        }

        [Test]
        public void RlzEncodeTest()
        {
            var encoded = RlzEncode(testInputData);
            Assert.That(encoded, Has.Length.EqualTo(rlzEncodedCurrentData.Length));
            for (var i = 0; i < rlzEncodedCurrentData.Length; i++)
            {
                Assert.That(encoded[i], Is.EqualTo(rlzEncodedCurrentData[i]).Within(0), $"Mismatch at index {i}");
            }
        }

        [Test]
        public void RlzDecodeOldImsDataTest()
        {
            var decoded = RlzDecode(rlzEncodedOldImsData, BinCount);

            for (var i = 0; i < BinCount; i++)
            {
                Assert.That(decoded[i], Is.EqualTo(testInputData[i]).Within(0), $"Mismatch at bin {i}");
            }
        }

        [Test]
        public void RlzEncodeShortTest()
        {
            var encoded = RlzEncode(testInputDataShort);
            Assert.That(encoded, Has.Length.EqualTo(rlzEncodedCurrentDataShort.Length));
            for (var i = 0; i < rlzEncodedCurrentDataShort.Length; i++)
            {
                Assert.That(encoded[i], Is.EqualTo(rlzEncodedCurrentDataShort[i]).Within(0), $"Mismatch at index {i}");
            }
        }

        [Test]
        public void RlzEncodeOldImsShortTest()
        {
            var encoded = RlzEncodeOld(testInputDataShort);
            Assert.That(encoded, Has.Length.EqualTo(rlzEncodedOldImsDataShort.Length));
            for (var i = 0; i < rlzEncodedOldImsDataShort.Length; i++)
            {
                Assert.That(encoded[i], Is.EqualTo(rlzEncodedOldImsDataShort[i]).Within(0), $"Mismatch at index {i}");
            }
        }

        private static int[] RlzDecode(IReadOnlyList<int> rlzData, int binCount)
        {
            var intensityArray = new int[binCount];
            var decoded = UIMFLibrary.RlzEncode.Decode(rlzData);
            foreach (var nzPoint in decoded)
            {
                if (nzPoint.Item1 < binCount)
                {
                    intensityArray[nzPoint.Item1] = nzPoint.Item2;
                }
                else
                {
                    Console.WriteLine("Warning: index out of bounds in RlzDecode: {0} > {1} ", nzPoint.Item1, binCount);
                    break;
                }
            }

            return intensityArray;
        }

        private static int[] RlzEncode(IReadOnlyList<int> intensities)
        {
            return UIMFLibrary.RlzEncode.Encode(intensities);
        }

        private static short[] RlzEncode(IReadOnlyList<short> intensities)
        {
            return UIMFLibrary.RlzEncode.Encode(intensities);
        }

        private short[] RlzEncodeOld(IList<short> intensities)
        {
            var rlzeDataList = new List<short>();
            var zeroCount = 0;

            // run length zero encoding
            for (var i = 0; i < intensities.Count; i++)
            {
                var intensity = intensities[i];
                if (intensity > 0)
                {
                    if (zeroCount < 0)
                    {
                        rlzeDataList.Add((short)zeroCount);
                        zeroCount = 0;
                    }

                    rlzeDataList.Add(intensity);
                }
                else
                {
                    if (zeroCount == short.MinValue)
                    {
                        // Too many zeros; need to append two points to rlzeDataList to avoid an overflow
                        rlzeDataList.Add((short)zeroCount);
                        rlzeDataList.Add(0); // This is the bug.
                        zeroCount = 0;
                    }

                    // Always count the zero
                    zeroCount--;
                }
            }
            // We don't care about any zeros/zeroCount after the last non-zero value; it's better if we don't append them to rlzeDataList.

            return rlzeDataList.ToArray();
        }
    }
}
