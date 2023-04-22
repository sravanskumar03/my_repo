results = tp.map(lambda x: my_function(fixed_arg1, fixed_arg2, x), dynamic_args)

# Close the ThreadPool
tp.close()
tp.join()
