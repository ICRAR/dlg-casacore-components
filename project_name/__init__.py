__package__ = "project_name"
# The following imports are the binding to the DALiuGE system
from dlg import droputils, utils

# extend the following as required
from .appComponents import MyAppDROP
from .dataComponents import MyDataDROP

__all__ = ["MyAppDROP", "MyDataDROP"]
