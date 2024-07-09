// Function to split an array into 4 smaller arrays
export function splitArrayIntoFour<T>(arr: T[]): T[][] {
    const result: T[][] = [];
    const chunkSize = Math.ceil(arr.length / 4); // Calculate the size of each chunk

    // Loop through the original array and slice it into chunks
    for (let i = 0; i < arr.length; i += chunkSize) {
        result.push(arr.slice(i, i + chunkSize));
    }

    return result;
}
