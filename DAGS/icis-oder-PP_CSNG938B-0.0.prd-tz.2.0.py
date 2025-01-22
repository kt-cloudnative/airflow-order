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
                , WORKFLOW_NAME='PP_CSNG938B',WORKFLOW_ID='a5de01c051d24c10a60d54bcaa05bdf9', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG938B-0.0.prd-tz.2.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 10, 55, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('a5de01c051d24c10a60d54bcaa05bdf9')

    csng938bJob_vol = []
    csng938bJob_volMnt = []
    csng938bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng938bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng938bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csng938bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csng938bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng938bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng938bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng938bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '3c5dbb0de77242339950ddbaea92b78f',
        'volumes': csng938bJob_vol,
        'volume_mounts': csng938bJob_volMnt,
        'env_from':csng938bJob_env,
        'task_id':'csng938bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=csng938bJob", "workDate=20241129", "duplExePram=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('a5de01c051d24c10a60d54bcaa05bdf9')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng938bJob,
        Complete
    ]) 

    # authCheck >> csng938bJob >> Complete
    workflow








