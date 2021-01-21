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




def safe_range_circle(lat_fundeio, lon_fundeio, radius, n_points):
    """Transform the Radius from lat/lon of mooring
    to a vector with 360 points locations in lat/lon,
    showing the limits of the buoy movement range."""

    r, r_lon = meters_to_degree(radius, lat_fundeio)
    lat = math.radians(lat_fundeio)
    lon = math.radians(lon_fundeio)

    pi = math.pi

    circle_points = [(lat + math.cos(2*pi/x)*r,lon+ math.sin(2*pi/x)*r) for x in range(1, n_points+1)]

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

    meters = round(meters, 3)
    km = round(km, 3)
    nm = round(nm, 3)
    yard = round(yard, 3)

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











