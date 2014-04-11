// --------------------------------------------------------------------------------------------------------------------
// <copyright file="FrameTypeInfo.cs" company="">
//   
// </copyright>
// <summary>
//   TODO The frame type info.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace UIMFLibrary
{
	/// <summary>
	/// TODO The frame type info.
	/// </summary>
	internal class FrameTypeInfo
	{
		#region Constructors and Destructors

		/// <summary>
		/// Initializes a new instance of the <see cref="FrameTypeInfo"/> class.
		/// </summary>
		/// <param name="numFramesInFile">
		/// TODO The num frames in file.
		/// </param>
		public FrameTypeInfo(int numFramesInFile)
		{
			this.NumFrames = 0;
			this.FrameIndexes = new int[numFramesInFile + 1];
		}

		#endregion

		#region Public Properties

		/// <summary>
		/// Gets the frame indexes.
		/// </summary>
		public int[] FrameIndexes { get; private set; }

		/// <summary>
		/// Gets the num frames.
		/// </summary>
		public int NumFrames { get; private set; }

		#endregion

		#region Public Methods and Operators

		/// <summary>
		/// TODO The add frame.
		/// </summary>
		/// <param name="frameNumber">
		/// TODO The frame number.
		/// </param>
		public void AddFrame(int frameNumber)
		{
			this.FrameIndexes[frameNumber] = this.NumFrames;
			this.NumFrames++;
		}

		#endregion
	}
}