#%%
import pyarrow.dataset as ds
import polars as pl

offset, limit = 937430, 100

dataset = ds.dataset('/home/flemm0/miggydb/test/audio_features', format='parquet')

files_dict = {}
cumulative_sum = 0
for i, file in enumerate(dataset.files):
    previous_sum = files_dict.get(dataset.files[i-1], (0, 0))[1] + 1 if i > 0 else 0
    cumulative_sum += pq.ParquetFile(file).metadata.num_rows
    files_dict[file] = (previous_sum, cumulative_sum)

filtered_files = list(files_dict.keys())
for file in files_dict.keys():
    if offset and files_dict[file][1] <= offset:
        filtered_files.remove(file)
    if limit and files_dict[file][0] >= offset + limit and file in filtered_files:
        filtered_files.remove(file)

files_dict = {key: val for key, val in files_dict.items() if key in filtered_files}

if len(files_dict) == 1:
    path = sorted(files_dict.keys(), key=lambda x: int(Path(x).stem.split('_')[-1]))[0]
    data = pl.read_parquet(path).tail(-(offset - files_dict[path][1])).head(limit)
    print(data)
else:
    head_path = sorted(files_dict.keys(), key=lambda x: int(Path(x).stem.split('_')[-1]))[0]
    tail_path = sorted(files_dict.keys(), key=lambda x: int(Path(x).stem.split('_')[-1]))[-1]

    # first read in from tail: the first (offset + length) - files_dict[tail_path][0] number of rows
    # then take the difference between head_nrows and n_rows read from tail_nrows and read in head
    tail_nrows = (offset + limit) - files_dict[tail_path][0]
    head_nrows = pq.ParquetFile(head_path).metadata.num_rows - tail_nrows

    data = pl.read_parquet(head_path, n_rows=head_nrows)
    data = data.extend(pl.read_parquet(tail_path, n_rows=tail_nrows))
    print(data)
# %%
