
import grid

# South-west coordinates for New York: lat 40.491577, lon -74.274776
# North-east coordinates for New York: lat 40.932936, lon -73.689754
nyc_bounds = {"SW": (40.491577, -74.274776), "NE": (40.932936, -73.689754)}
lat_grid_bounds, lon_grid_bounds = grid.get_grid_block_boundaries(
    nyc_bounds["SW"], nyc_bounds["NE"], 494, 49)
grid_bounds = grid.get_grid_square_bounds(lat_grid_bounds, lon_grid_bounds)
grid.print_grid_csv('grid_bounds.csv', grid_bounds)
