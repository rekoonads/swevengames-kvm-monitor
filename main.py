import os
import time
import json
import importlib
import traceback

import schedule

from connection import create_influxdb_point, write_api, INFLUX_BUCKET, INFLUX_ORG
from modules import MONITORING_INTERVAL, logger

# Try to import libvirt to check if it's available
try:
    import libvirt
    LIBVIRT_AVAILABLE = True
except ImportError:
    LIBVIRT_AVAILABLE = False


def load_config():
    with open('config/modules_config.json', 'r') as f:
        config = json.load(f)

    # Get modules - handle both string format and dict format
    modules = config.get("modules", [])

    # Convert dict format to string format if needed
    if modules and isinstance(modules[0], dict):
        module_names = [m['name'] for m in modules]
    else:
        module_names = modules

    # Filter out kvm_monitor if libvirt is not available
    if not LIBVIRT_AVAILABLE and "kvm_monitor" in module_names:
        module_names = [m for m in module_names if m != "kvm_monitor"]
        logger.warning("libvirt not available. Disabling kvm_monitor module.")

    return module_names


def run_module(module_name):
    try:
        module = importlib.import_module(f"modules.{module_name}")
        data = module.collect_data()

        if module_name == 'kvm_monitor' and not LIBVIRT_AVAILABLE:
            return
            
        if data:
            if isinstance(data, list):
                # If collect data return multiple records
                for record in data:
                    point = create_influxdb_point(module_name, record)
                    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
            else:
                point = create_influxdb_point(module_name, data)
                write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
    except Exception as e:
        logger.error(f"Error running module {module_name}: {str(e)}")
        logger.debug(traceback.format_exc())



def main():
    logger.info("Starting monitoring service")
    modules = load_config()
    
    if not LIBVIRT_AVAILABLE and "kvm_monitor" in modules:
        logger.warning("KVM monitor module is configured but libvirt is not available")
    
    for module in modules:
        schedule.every(MONITORING_INTERVAL).seconds.do(run_module, module)
        logger.info(f"Scheduled {module} module")

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
