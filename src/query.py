"""
****************************************
 * @author: Chandler Qian
 * Date: 3/11/21
 * Project: the project this script belongs to
 * Purpose: queries for autoencoders
 * Python version: 3.8.1
 * Project root: /home/usr/projects/safety-recommendation
 * Environment package: safety_rec on the remote
****************************************
"""
import yaml

with open("./autoencoder/config.yaml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

query_raw_fea = """
SELECT veh.HardwareId,
       veh.Vin,
       Make,
       Model,
       Year,
       VehicleType,
       WeightClass,
       trip.HoursInterval,
       trip.DistanceInterval,
       voc.VocationId,
       eng.HorsepowerInterval,
       eng.DisplacementInterval,
       speed.* EXCEPT (
       HardwareId,
       travelHours),
       tem.* EXCEPT (
       HardwareId,
       travelHours),
       Precip.* EXCEPT (
       HardwareId,
       travelHours),
       snow.* EXCEPT (
       HardwareId,
       travelHours),
       press.* EXCEPT (
       HardwareId,
       travelHours),
       geo.* EXCEPT (
       HardwareId),
       battery.* EXCEPT (
       HardwareId,
       Vin,
       TotalDays),
       CASE
           WHEN OilChangeInvertal IS NOT NULL THEN OilChangeInvertal
           ELSE 0
           END AS OilChangeInvertal
FROM `geotab-bi.Safety.Hardwareid_Vin_Decode` veh
         INNER JOIN
     `geotab-bi.Safety.DrivingHours_Distance_Interval` trip
     USING
         (Hardwareid)
         INNER JOIN
     `geotab-bi.Safety.Hardwareid_Vocation` voc
     USING
         (Hardwareid)
         INNER JOIN
     `geotab-bi.Safety.EngineDetails_Interval` eng
     USING
         (Hardwareid, Vin)
         INNER JOIN
     `geotab-bi.Safety.SpeedDistByVehicle` speed
     USING
         (Hardwareid)
         INNER JOIN
     `geotab-bi.Safety.TemperatureDistribution` tem
     USING
         (Hardwareid)
         INNER JOIN
     `geotab-bi.Safety.PrecipitationDistribution` Precip
     USING
         (Hardwareid)
         INNER JOIN
     `geotab-bi.Safety.SnowDistribution` snow
     USING
         (Hardwareid)
         INNER JOIN
     `geotab-bi.Safety.PressureDistribution` press
     USING
         (Hardwareid)
         INNER JOIN
     `geotab-bi.Safety.Geo2_Geo3` geo
     USING
         (Hardwareid)
         LEFT JOIN
     `geotab-bi.Safety.BatteryDistribution` battery
     USING
         (Hardwareid, Vin)
         LEFT JOIN
     `geotab-bi.Safety.EngineOilChange_Count_Interval` oil
     USING
         (Hardwareid)
"""

"""
The feature dictionary used for data preprocessing
"""
query_fea_dict = """
select * from {fea_dict}
""".format(fea_dict=cfg['tables']['fea_dict'])

"""
The imputed raw data from the data pipeline
"""
query_imputed_fea = """
select *except(OilChangeInterval)
, case when OilChangeInterval = 999999 then 0 else OilChangeInterval end as OilChangeInterval  
from {imputed_raw_data}
""".format(imputed_raw_data=cfg['tables']['imputed_raw_data'])

"""
The non raw data from the data pipeline
"""
query_fea = """
select * from {raw_data}
""".format(raw_data=cfg['tables']['raw_data'])
