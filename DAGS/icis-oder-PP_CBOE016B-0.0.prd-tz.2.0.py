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
                , WORKFLOW_NAME='PP_CBOE016B',WORKFLOW_ID='785a64a5800b475488c185287dea2fdf', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOE016B-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 17, 30, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('785a64a5800b475488c185287dea2fdf')

    cboe016bJob_vol = []
    cboe016bJob_volMnt = []
    cboe016bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cboe016bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cboe016bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cboe016bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cboe016bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cboe016bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'e37965c2b537406b899e4eefdcd2fbc0',
        'volumes': cboe016bJob_vol,
        'volume_mounts': cboe016bJob_volMnt,
        'env_from':cboe016bJob_env,
        'task_id':'cboe016bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.57',
        'arguments':["--job.name=cboe016bJob"
, "requestDate=${YYYYMMDDHHMISSSSS}"
, "yyyyMm=202406", "svcName=cboe016bJob"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('785a64a5800b475488c185287dea2fdf')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cboe016bJob,
        Complete
    ]) 

    # authCheck >> cboe016bJob >> Complete
    workflow








