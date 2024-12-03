# example
EXP_NAME="output/output_exp_10w_"
DB=1

## id
python main_redis.py \
    --db $DB \
    --output "${EXP_NAME}1" \
    --id 257493573

## latitude
python main_redis.py \
    --db $DB \
    --output "${EXP_NAME}2" \
    --latitude 42.613358

## amentity
python main_redis.py \
    --db $DB \
    --output "${EXP_NAME}3" \
    --amenity school

## bbox
python main_redis.py \
    --db $DB \
    --output "${EXP_NAME}4" \
    --bounding-box "10,-90,90,-1"

## radius
python main_redis.py \
    --db $DB \
    --output "${EXP_NAME}5" \
    --latitude 42 \
    --longitude -71 \
    --radius 100

## bbox + amentity
python main_redis.py \
    --db $DB \
    --output "${EXP_NAME}6" \
    --bounding-box "10,-90,90,-1" \
    --amenity school

## radius + phone
python main_redis.py \
    --db $DB \
    --output "${EXP_NAME}7" \
    --latitude 42 \
    --longitude -71 \
    --radius 100 \
    --phone "+1 781 224 3780"


## radius + amentity
python main_redis.py \
    --db $DB \
    --output "${EXP_NAME}8" \
    --latitude 42 \
    --longitude -71 \
    --radius 100 \
    --amenity school
