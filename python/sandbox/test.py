import numpy as np

# Create large arrays of random floats
array_float32 = np.random.rand(1000000).astype(np.float32)
array_float16 = np.random.rand(1000000).astype(np.float16)

# Calculate the memory size in bytes
size_float32 = array_float32.nbytes
size_float16 = array_float16.nbytes

print(f"Memory usage for float32 array: {size_float32} bytes")
print(f"Memory usage for float16 array: {size_float16} bytes")