export const genid = (): number => {
    const timestamp = Date.now(); // Current timestamp in milliseconds
    const randomComponent = Math.floor(Math.random() * 1000); // Random number between 0 and 999
    return timestamp + randomComponent; // Combine them
  };
  