﻿/*
 * Fewer allocations version:
 * Copyright (c) 2016 Chase Pettit <chasepettit@gmail.com>
 *
 * Improved version to C# LibLZF Port:
 * Copyright (c) 2010 Roman Atachiants <kelindar@gmail.com>
 *
 * Original CLZF Port:
 * Copyright (c) 2005 Oren J. Maurice <oymaurice@hazorea.org.il>
 *
 * Original LibLZF Library  Algorithm:
 * Copyright (c) 2000-2008 Marc Alexander Lehmann <schmorp@schmorp.de>
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 *   1.  Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *
 *   2.  Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 *   3.  The name of the author may not be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO
 * EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

using System;

// ReSharper disable UnusedMember.Global

namespace UIMFLibrary
{
    /// <summary>
    /// Improved C# LZF Compressor, a very small data compression library. The compression algorithm is extremely fast.
    /// </summary>
    public static class CLZF2
    {
        // Ignore Spelling: clzf

        #region Tunable Constants

        /// <summary>
        /// Multiple of input size used to estimate work/output buffer size.
        /// Larger values increase initial memory usage but potentially reduces number of allocations.
        /// </summary>
        private const int BUFFER_SIZE_ESTIMATE = 2;

        /// <summary>
        /// Size of hash table is 2^HLOG bytes.
        /// Decompression is independent of the hash table size.
        /// The difference between 15 and 14 is very small
        /// for small blocks (and 14 is usually a bit faster).
        /// For a low-memory/faster configuration, use HLOG == 13;
        /// For best compression, use 15 or 16 (or more, up to 22).
        /// </summary>
        private const uint HLOG = 14;

        #endregion

        # region Other Constants (Do Not Modify)

        private const uint HSIZE = 1 << (int)HLOG;
        private const uint MAX_LIT = 1 << 5;
        private const uint MAX_OFF = 1 << 13;
        private const uint MAX_REF = (1 << 8) + (1 << 3);

        #endregion

        #region Fields

        /// <summary>
        /// Hash table, that can be allocated only once.
        /// </summary>
        private static readonly long[] HashTable = new long[HSIZE];

        /// <summary>
        /// Lock object for access to the hash table so that we can keep things thread safe.
        /// Still up to the caller to make sure any shared outputBuffer use is thread safe.
        /// </summary>
        private static readonly object locker = new object();

        #endregion

        #region Public Methods

        /// <summary>
        /// Compress input bytes.
        /// </summary>
        /// <param name="inputBytes">Bytes to compress (minimum length: 2)</param>
        /// <returns>Compressed bytes</returns>
        public static byte[] Compress(byte[] inputBytes)
        {
            return Compress(inputBytes, inputBytes.Length);
        }

        /// <summary>
        /// Compress input bytes.
        /// </summary>
        /// <param name="inputBytes">Bytes to compress (minimum length: 2)</param>
        /// <param name="inputLength">Length of data in inputBytes to decompress</param>
        /// <returns>Compressed bytes</returns>
        public static byte[] Compress(byte[] inputBytes, int inputLength)
        {
            byte[] tempBuffer = null;
            var byteCount = Compress(inputBytes, ref tempBuffer, inputLength);

            var outputBytes = new byte[byteCount];
            Buffer.BlockCopy(tempBuffer, 0, outputBytes, 0, byteCount);
            return outputBytes;
        }

        /// <summary>
        /// Compress input bytes.
        /// </summary>
        /// <param name="inputBytes">Bytes to compress (minimum length: 2)</param>
        /// <param name="outputBuffer">Output/work buffer. Upon completion, will contain the output</param>
        /// <returns>Length of output</returns>
        public static int Compress(byte[] inputBytes, ref byte[] outputBuffer)
        {
            return Compress(inputBytes, ref outputBuffer, inputBytes.Length);
        }

        /// <summary>
        /// Compress input bytes.
        /// </summary>
        /// <param name="inputBytes">Bytes to compress (minimum length: 2)</param>
        /// <param name="outputBuffer">Output/work buffer. Upon completion, will contain the output</param>
        /// <param name="inputLength">Length of data in inputBytes</param>
        /// <returns>Length of output</returns>
        public static int Compress(byte[] inputBytes, ref byte[] outputBuffer, int inputLength)
        {
            // Estimate necessary output buffer size.
            var outputByteCountGuess = inputBytes.Length * BUFFER_SIZE_ESTIMATE;
            if (outputBuffer == null || outputBuffer.Length < outputByteCountGuess)
                outputBuffer = new byte[outputByteCountGuess];

            var byteCount = LZF_Compress(inputBytes, ref outputBuffer, inputLength);

            // If byteCount is 0, increase buffer size and try again.
            while (byteCount == 0)
            {
                outputByteCountGuess *= 2;
                outputBuffer = new byte[outputByteCountGuess];
                byteCount = LZF_Compress(inputBytes, ref outputBuffer, inputLength);
            }

            return byteCount;
        }

        /// <summary>
        /// Decompress input bytes.
        /// </summary>
        /// <param name="inputBytes">Bytes to decompress</param>
        /// <returns>Decompressed bytes</returns>
        public static byte[] Decompress(byte[] inputBytes)
        {
            return Decompress(inputBytes, inputBytes.Length);
        }

        /// <summary>
        /// Decompress input bytes.
        /// </summary>
        /// <param name="inputBytes">Bytes to decompress</param>
        /// <param name="inputLength">Length of data in inputBytes to decompress</param>
        /// <returns>Decompressed bytes</returns>
        public static byte[] Decompress(byte[] inputBytes, int inputLength)
        {
            byte[] tempBuffer = null;
            var byteCount = Decompress(inputBytes, ref tempBuffer, inputLength);

            var outputBytes = new byte[byteCount];
            Buffer.BlockCopy(tempBuffer, 0, outputBytes, 0, byteCount);
            return outputBytes;
        }

        /// <summary>
        /// Decompress input bytes.
        /// </summary>
        /// <param name="inputBytes">Bytes to decompress</param>
        /// <param name="outputBuffer">Output/work buffer. Upon completion, will contain the output</param>
        /// <returns>Length of output</returns>
        public static int Decompress(byte[] inputBytes, ref byte[] outputBuffer)
        {
            return Decompress(inputBytes, ref outputBuffer, inputBytes.Length);
        }

        /// <summary>
        /// Decompress input bytes.
        /// </summary>
        /// <param name="inputBytes">Bytes to decompress</param>
        /// <param name="outputBuffer">Output/work buffer. Upon completion, will contain the output</param>
        /// <param name="inputLength">Length of data in inputBytes</param>
        /// <returns>Length of output</returns>
        public static int Decompress(byte[] inputBytes, ref byte[] outputBuffer, int inputLength)
        {
            // Estimate necessary output buffer size.
            var outputByteCountGuess = inputBytes.Length * BUFFER_SIZE_ESTIMATE;
            if (outputBuffer == null || outputBuffer.Length < outputByteCountGuess)
                outputBuffer = new byte[outputByteCountGuess];

            var byteCount = LZF_Decompress(inputBytes, ref outputBuffer, inputLength);

            // If byteCount is 0, increase buffer size and try again.
            while (byteCount == 0)
            {
                outputByteCountGuess *= 2;
                outputBuffer = new byte[outputByteCountGuess];
                byteCount = LZF_Decompress(inputBytes, ref outputBuffer, inputLength);
            }

            return byteCount;
        }

        #endregion

        #region Private Methods

        /// <summary>
        /// Compresses the data using LibLZF algorithm.
        /// </summary>
        /// <param name="input">Reference to the data to compress (minimum length: 2)</param>
        /// <param name="output">Reference to a buffer which will contain the compressed data</param>
        /// <param name="inputLength">Length of input bytes to process</param>
        /// <returns>The size of the compressed archive in the output buffer</returns>
        // ReSharper disable once SuggestBaseTypeForParameter
        private static int LZF_Compress(byte[] input, ref byte[] output, int inputLength)
        {
            var outputLength = output.Length;

            uint inputIndex = 0;
            uint outputIndex = 0;

            var hValue = (uint)((input[inputIndex] << 8) | input[inputIndex + 1]); // FRST(in_data, inputIndex);
            var lit = 0;

            // Lock so we have exclusive access to the hash table.
            lock (locker)
            {
                Array.Clear(HashTable, 0, (int)HSIZE);

                while (true)
                {
                    if (inputIndex < inputLength - 2)
                    {
                        hValue = (hValue << 8) | input[inputIndex + 2];
                        long hSlot = (hValue ^ (hValue << 5)) >> (int)(3 * 8 - HLOG - hValue * 5) & (HSIZE - 1);
                        var reference = HashTable[hSlot];
                        HashTable[hSlot] = inputIndex;

                        long offset;
                        if ((offset = inputIndex - reference - 1) < MAX_OFF
                            && inputIndex + 4 < inputLength
                            && reference > 0
                            && input[reference + 0] == input[inputIndex + 0]
                            && input[reference + 1] == input[inputIndex + 1]
                            && input[reference + 2] == input[inputIndex + 2])
                        {
                            /* match found at *reference++ */
                            uint len = 2;
                            var maxLength = (uint)inputLength - inputIndex - len;
                            maxLength = maxLength > MAX_REF ? MAX_REF : maxLength;

                            if (outputIndex + lit + 1 + 3 >= outputLength)
                                return 0;

                            do
                            {
                                len++;
                            } while (len < maxLength && input[reference + len] == input[inputIndex + len]);

                            if (lit != 0)
                            {
                                output[outputIndex++] = (byte)(lit - 1);
                                lit = -lit;
                                do
                                {
                                    output[outputIndex++] = input[inputIndex + lit];
                                } while (++lit != 0);
                            }

                            len -= 2;
                            inputIndex++;

                            if (len < 7)
                            {
                                output[outputIndex++] = (byte)((offset >> 8) + (len << 5));
                            }
                            else
                            {
                                output[outputIndex++] = (byte)((offset >> 8) + (7 << 5));
                                output[outputIndex++] = (byte)(len - 7);
                            }

                            output[outputIndex++] = (byte)offset;

                            inputIndex += len - 1;
                            hValue = (uint)((input[inputIndex] << 8) | input[inputIndex + 1]);

                            hValue = (hValue << 8) | input[inputIndex + 2];
                            HashTable[(hValue ^ (hValue << 5)) >> (int)(3 * 8 - HLOG - hValue * 5) & (HSIZE - 1)] = inputIndex;
                            inputIndex++;

                            hValue = (hValue << 8) | input[inputIndex + 2];
                            HashTable[(hValue ^ (hValue << 5)) >> (int)(3 * 8 - HLOG - hValue * 5) & (HSIZE - 1)] = inputIndex;
                            inputIndex++;
                            continue;
                        }
                    }
                    else if (inputIndex == inputLength)
                    {
                        break;
                    }

                    /* one more literal byte we must copy */
                    lit++;
                    inputIndex++;

                    if (lit == MAX_LIT)
                    {
                        if (outputIndex + 1 + MAX_LIT >= outputLength)
                            return 0;

                        output[outputIndex++] = (byte)(MAX_LIT - 1);
                        lit = -lit;
                        do
                        {
                            output[outputIndex++] = input[inputIndex + lit];
                        } while (++lit != 0);
                    }
                } // for
            } // lock

            if (lit != 0)
            {
                if (outputIndex + lit + 1 >= outputLength)
                    return 0;

                output[outputIndex++] = (byte)(lit - 1);
                lit = -lit;
                do
                {
                    output[outputIndex++] = input[inputIndex + lit];
                } while (++lit != 0);
            }

            return (int)outputIndex;
        }

        /// <summary>
        /// Decompresses the data using LibLZF algorithm.
        /// </summary>
        /// <param name="input">Reference to the data to decompress</param>
        /// <param name="output">Reference to a buffer which will contain the decompressed data</param>
        /// <param name="inputLength">Length of input bytes to process</param>
        /// <returns>The size of the decompressed archive in the output buffer</returns>
        // ReSharper disable once SuggestBaseTypeForParameter
        private static int LZF_Decompress(byte[] input, ref byte[] output, int inputLength)
        {
            var outputLength = output.Length;

            uint inputIndex = 0;
            uint outputIndex = 0;

            do
            {
                uint ctrl = input[inputIndex++];

                if (ctrl < 1 << 5) /* literal run */
                {
                    ctrl++;

                    if (outputIndex + ctrl > outputLength)
                    {
                        //SET_ERRNO (E2BIG);
                        return 0;
                    }

                    do
                    {
                        output[outputIndex++] = input[inputIndex++];
                    } while (--ctrl != 0);
                }
                else /* back reference */
                {
                    var len = ctrl >> 5;

                    var reference = (int)(outputIndex - ((ctrl & 0x1f) << 8) - 1);

                    if (len == 7)
                        len += input[inputIndex++];

                    reference -= input[inputIndex++];

                    if (outputIndex + len + 2 > outputLength)
                    {
                        //SET_ERRNO (E2BIG);
                        return 0;
                    }

                    if (reference < 0)
                    {
                        //SET_ERRNO (EINVAL);
                        return 0;
                    }

                    output[outputIndex++] = output[reference++];
                    output[outputIndex++] = output[reference++];

                    do
                    {
                        output[outputIndex++] = output[reference++];
                    } while (--len != 0);
                }
            }
            while (inputIndex < inputLength);

            return (int)outputIndex;
        }

        #endregion

    }
}
