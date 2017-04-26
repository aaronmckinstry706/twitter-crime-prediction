
def get_grid_block_boundaries(south_west_coordinates, north_east_coordinates,
                              n):
    south_west_coordinates = [float(x) for x in south_west_coordinates]
    north_east_coordinates = [float(x) for x in north_east_coordinates]
    latitude_boundaries = [south_west_coordinates[0], north_east_coordinates[0]]
    latitude_range = latitude_boundaries[1] - latitude_boundaries[0]
    longitude_boundaries = [south_west_coordinates[1],
                            north_east_coordinates[1]]
    longitude_range = longitude_boundaries[1] - longitude_boundaries[0]
    for i in range(1, n):
        new_latitude_boundary = latitude_boundaries[0] + i*latitude_range/n
        latitude_boundaries.insert(-1, new_latitude_boundary)
        new_longitude_boundary = longitude_boundaries[0] + i*longitude_range/n
        longitude_boundaries.insert(-1, new_longitude_boundary)
    return latitude_boundaries, longitude_boundaries

def split_record(s):
    fields = s.split(',')
    functions = [int, int, int, float, float,
                 lambda x: x, int, lambda x: x, lambda x: x]
    for i in range(len(fields)):
        fields[i] = functions[i](fields[i])
    return fields

