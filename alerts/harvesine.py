import math




def radius_buoy(lat, lon, depth):
    return None




def meters_to_degree(meters_distance, latitude):
    """Considering distance of 1 degree lat/lon
    as 111.111 meters of distance...
    """


    lat_degrees = meters_distance/111111
    lon_degrees = meters_distance/(111111 * abs(math.cos(latitude)))

    return lat_degrees, lon_degrees




def safe_range_circle(lat_fundeio, lon_fundeio, radius):
    """Transform the Radius from lat/lon of mooring
    to a vector with 360 points locations in lat/lon,
    showing the limits of the buoy movement range."""

    import numpy as np

    pi = math.pi


    r, r_lon = meters_to_degree(radius, lat_fundeio)
    lat = math.radians(lat_fundeio)
    lon = math.radians(lon_fundeio)
    r_radians = math.radians(r)

    points = np.linspace(0, 2*pi, 360)



    circle_points = [(lat + math.sin(x)*r_radians,lon - math.cos(x)*r_radians) for x in points]

    # radians to degree
    circle_lat = [(x[0]*180/pi) for x in circle_points]
    circle_lon = [(x[1]*180/pi) for x in circle_points]

    return circle_lat, circle_lon





# Function from the link: https://community.esri.com/t5/coordinate-reference-systems/distance-on-a-sphere-the-haversine-formula/ba-p/902128
def haversine(coord1: object, coord2: object):
    import math

    # Coordinates in decimal degrees (e.g. 2.89078, 12.79797)
    lon1, lat1 = coord1
    lon2, lat2 = coord2

    R = 6371000  # radius of Earth in meters
    phi_1 = math.radians(lat1)
    phi_2 = math.radians(lat2)

    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2.0) ** 2 + math.cos(phi_1) * math.cos(
        phi_2) * math.sin(delta_lambda / 2.0) ** 2

    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    meters = R * c  # output distance in meters
    km = meters / 1000.0  # output distance in kilometers
    nm = meters / 1852 # nautic miles
    yard = meters * 1.094

    meters = round(meters)
    km = round(km, 3)
    nm = round(nm, 1)
    yard = round(yard, 1)

    print(f"Distance: {meters} m")
    print(f"Distance: {km} km")
    print(f"Distance: {nm} nautic miles")
    print(f"Distance: {yard} yards")

    return {'km':km, 'meters':meters, 'nm':nm, 'yards':yard}





def safe_position(radius_safe, distance_from_point):
    """Check if distance of currently position is greater than safe distance
       radius and distance in METERS"""


    if distance_from_point < radius_safe:
        situation = ["Buoy in Position", "OK"]

    elif distance_from_point > radius_safe:
        situation = ["Buoy Out Position!", "ALERT"]

    elif distance_from_point == radius_safe:
        # Probably we will never see this case in real
        situation = ["Buoy on Limit Position", "ON LIMIT"]

    return situation



def find_centroid(lon_points, lat_points):


    import numpy as np

    n_points = len(lat_points)

    sum_lon = np.sum(lon_points)
    sum_lat = np.sum(lat_points)

    center_lon = sum_lon/n_points
    center_lat = sum_lat/n_points

    return center_lon, center_lat




def find_outer_points(lon_points, lat_points, lat_center, lon_center):
    '''Find the most outer point for each degree
    within a circle.'''

    import math
    pi = math.pi


    points = np.linspace(0, 2*pi, 360)

    lat = math.radians(center_lat)
    lon = math.radians(center_lat)
    r_radians = math.radians(1)

    circle_points = [(lat + math.sin(x)*r_radians,lon + math.cos(x)*r_radians) for x in points]

    final_coords = []
    for point in circle_points:
        lat_point = circle_points[0]
        lon_point = circle_points[1]

        lat_diff = lat_points - lat_point
        lon_diff = lon_points - lon_point

        final_lat = max()











