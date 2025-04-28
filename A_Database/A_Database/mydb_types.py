from typing import Dict, List, Union, Any

Data = Dict[str, str]
Record = Dict[str, str]
Records = Dict[str, Record]
Index = Dict[str, List[str]]
Indexes = Dict[str, Dict[str, Union[Index, Any]]]
Conditions = Dict[str, Union[str, Dict[str, Union[float, List[str]]]]]
BulkData = List[Data]
ExplainPlan = Dict[str, str]