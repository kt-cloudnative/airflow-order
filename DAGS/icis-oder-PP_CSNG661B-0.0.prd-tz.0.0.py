from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='PP_CSNG661B',WORKFLOW_ID='b1d19832b0334093b8f4f9830113d9d3', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG661B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 3, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('b1d19832b0334093b8f4f9830113d9d3')

    csng661bJob_vol = []
    csng661bJob_volMnt = []
    csng661bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng661bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng661bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng661bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'e0c7eb24fe8341748bf3ddbf8bf49899',
        'volumes': csng661bJob_vol,
        'volume_mounts': csng661bJob_volMnt,
        'env_from':csng661bJob_env,
        'task_id':'csng661bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
        'arguments':["--job.name=csng661bJob", "requestDate=${YYYYMMDDHHMISS}", "pgmNm=csng661b", "reqType=A", "workEmpNo=91348440", "workEmpName=범정부자격변동", "workOfcCd=710850"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('b1d19832b0334093b8f4f9830113d9d3')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng661bJob,
        Complete
    ]) 

    # authCheck >> csng661bJob >> Complete
    workflow








