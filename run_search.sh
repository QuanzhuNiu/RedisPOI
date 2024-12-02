python main_redis.py \
    --db 0 \
    --output output_school_in_bbox \
    --bounding-box 10,-10,50,-100 \
    --amenity school 

python main_redis.py \
    --db 0 \
    --output output_fuel_in_radius \
    --latitude -70 \
    --longitude 20 \
    --radius 100 \
    --amenity fuel 
 