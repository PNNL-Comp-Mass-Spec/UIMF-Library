// --------------------------------------------------------------------------------------------------------------------
// <summary>
//   Improved C# LZF Compressor, a very small data compression library. The compression algorithm is extremely fast.
// </summary>
// --------------------------------------------------------------------------------------------------------------------
namespace UIMFLibrary
{
	using System;

	/* Benchmark with Alice29 Canterbury Corpus
		---------------------------------------
		(Compression) Original CLZF C#
		Raw = 152089, Compressed = 101092
		 8292,4743 ms.
		---------------------------------------
		(Compression) My LZF C#
		Raw = 152089, Compressed = 101092
		 33,0019 ms.
		---------------------------------------
		(Compression) Zlib using SharpZipLib
		Raw = 152089, Compressed = 54388
		 8389,4799 ms.
		---------------------------------------
		(Compression) QuickLZ C#
		Raw = 152089, Compressed = 83494
		 80,0046 ms.
		---------------------------------------
		(Decompression) Original CLZF C#
		Decompressed = 152089
		 16,0009 ms.
		---------------------------------------
		(Decompression) My LZF C#
		Decompressed = 152089
		 15,0009 ms.
		---------------------------------------
		(Decompression) Zlib using SharpZipLib
		Decompressed = 152089
		 3577,2046 ms.
		---------------------------------------
		(Decompression) QuickLZ C#
		Decompressed = 152089
		 21,0012 ms.
	*/

	/// <summary>
	/// Improved C# LZF Compressor, a very small data compression library. The compression algorithm is extremely fast. 
	/// </summary>
	public sealed class LZFCompressionUtil
	{
		#region Constants

		/// <summary>
		/// H Log
		/// </summary>
		private const uint HLOG = 14;

		/// <summary>
		/// H Size
		/// </summary>
		private const uint HSIZE = 1 << 14;

		/// <summary>
		/// max lit
		/// </summary>
		private const uint MAX_LIT = 1 << 5;

		/// <summary>
		/// Mxx off
		/// </summary>
		private const uint MAX_OFF = 1 << 13;

		/// <summary>
		/// Max ref
		/// </summary>
		private const uint MAX_REF = (1 << 8) + (1 << 3);

		#endregion

		#region Static Fields

		/// <summary>
		/// Hashtable, thac can be allocated only once
		/// </summary>
		private static readonly long[] HashTable = new long[HSIZE];

		#endregion

		#region Public Methods and Operators

		/// <summary>
		/// Compresses the data using LibLZF algorithm
		/// </summary>
		/// <param name="input">
		/// Reference to the data to compress
		/// </param>
		/// <param name="inputLength">
		/// Lenght of the data to compress
		/// </param>
		/// <param name="output">
		/// Reference to a buffer which will contain the compressed data
		/// </param>
		/// <param name="outputLength">
		/// Lenght of the compression buffer (should be bigger than the input buffer)
		/// </param>
		/// <returns>
		/// The size of the compressed archive in the output buffer
		/// </returns>
		public static int Compress(ref byte[] input, int inputLength, ref byte[] output, int outputLength)
		{
			Array.Clear(HashTable, 0, (int)HSIZE);

			long hslot;
			uint iidx = 0;
			uint oidx = 0;
			long reference;

			var hval = (uint)((input[iidx] << 8) | input[iidx + 1]); // FRST(in_data, iidx);
			long off;
			var lit = 0;

			for (;;)
			{
				if (iidx < inputLength - 2)
				{
					hval = (hval << 8) | input[iidx + 2];
					hslot = (hval ^ (hval << 5)) >> (int)(((3 * 8 - HLOG)) - hval * 5) & (HSIZE - 1);
					reference = HashTable[hslot];
					HashTable[hslot] = (long)iidx;

					if ((off = iidx - reference - 1) < MAX_OFF && iidx + 4 < inputLength && reference > 0
					    && input[reference + 0] == input[iidx + 0] && input[reference + 1] == input[iidx + 1]
					    && input[reference + 2] == input[iidx + 2])
					{
						/* match found at *reference++ */
						uint len = 2;
						var maxlen = (uint)inputLength - iidx - len;
						maxlen = maxlen > MAX_REF ? MAX_REF : maxlen;

						if (oidx + lit + 1 + 3 >= outputLength)
						{
							return 0;
						}

						do len++;
						while (len < maxlen && input[reference + len] == input[iidx + len]);

						if (lit != 0)
						{
							output[oidx++] = (byte)(lit - 1);
							lit = -lit;
							do output[oidx++] = input[iidx + lit];
							while ((++lit) != 0);
						}

						len -= 2;
						iidx++;

						if (len < 7)
						{
							output[oidx++] = (byte)((off >> 8) + (len << 5));
						}
						else
						{
							output[oidx++] = (byte)((off >> 8) + (7 << 5));
							output[oidx++] = (byte)(len - 7);
						}

						output[oidx++] = (byte)off;

						iidx += len - 1;
						hval = (uint)((input[iidx] << 8) | input[iidx + 1]);

						hval = (hval << 8) | input[iidx + 2];
						HashTable[(hval ^ (hval << 5)) >> (int)(((3 * 8 - HLOG)) - hval * 5) & (HSIZE - 1)] = iidx;
						iidx++;

						hval = (hval << 8) | input[iidx + 2];
						HashTable[(hval ^ (hval << 5)) >> (int)(((3 * 8 - HLOG)) - hval * 5) & (HSIZE - 1)] = iidx;
						iidx++;
						continue;
					}
				}
				else if (iidx == inputLength)
				{
					break;
				}

				/* one more literal byte we must copy */
				lit++;
				iidx++;

				if (lit == MAX_LIT)
				{
					if (oidx + 1 + MAX_LIT >= outputLength)
					{
						return 0;
					}

					output[oidx++] = (byte)(MAX_LIT - 1);
					lit = -lit;
					do output[oidx++] = input[iidx + lit];
					while ((++lit) != 0);
				}
			}

			if (lit != 0)
			{
				if (oidx + lit + 1 >= outputLength)
				{
					return 0;
				}

				output[oidx++] = (byte)(lit - 1);
				lit = -lit;
				do output[oidx++] = input[iidx + lit];
				while ((++lit) != 0);
			}

			return (int)oidx;
		}

		/// <summary>
		/// Decompresses the data using LibLZF algorithm
		/// </summary>
		/// <param name="input">
		/// Reference to the data to decompress
		/// </param>
		/// <param name="inputLength">
		/// Lenght of the data to decompress
		/// </param>
		/// <param name="output">
		/// Reference to a buffer which will contain the decompressed data
		/// </param>
		/// <param name="outputLength">
		/// The size of the decompressed archive in the output buffer
		/// </param>
		/// <returns>
		/// Returns decompressed size
		/// </returns>
		public static int Decompress(ref byte[] input, int inputLength, ref byte[] output, int outputLength)
		{
			uint iidx = 0;
			uint oidx = 0;

			do
			{
				uint ctrl = input[iidx++];

				if (ctrl < (1 << 5)) /* literal run */
				{
					ctrl++;

					if (oidx + ctrl > outputLength)
					{
						// SET_ERRNO (E2BIG);
						return 0;
					}

					do output[oidx++] = input[iidx++];
					while ((--ctrl) != 0);
				}
				else /* back reference */
				{
					var len = ctrl >> 5;

					var reference = (int)(oidx - ((ctrl & 0x1f) << 8) - 1);

					if (len == 7)
					{
						len += input[iidx++];
					}

					reference -= input[iidx++];

					if (oidx + len + 2 > outputLength)
					{
						// SET_ERRNO (E2BIG);
						return 0;
					}

					if (reference < 0)
					{
						// SET_ERRNO (EINVAL);
						return 0;
					}

					output[oidx++] = output[reference++];
					output[oidx++] = output[reference++];

					do output[oidx++] = output[reference++];
					while ((--len) != 0);
				}
			}
			while (iidx < inputLength);

			return (int)oidx;
		}

		#endregion
	}
}