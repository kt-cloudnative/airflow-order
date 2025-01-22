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
                , WORKFLOW_NAME='PP_CBON313B',WORKFLOW_ID='1a7191175b5748f0958e36f0bf3bcec5', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBON313B-0.0.prd-tz.5.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 40, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('1a7191175b5748f0958e36f0bf3bcec5')

    cbon313bJob_vol = []
    cbon313bJob_volMnt = []
    cbon313bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbon313bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbon313bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbon313bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbon313bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbon313bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'a6d4605b50b4430e86d63c53de47ddcc',
        'volumes': cbon313bJob_vol,
        'volume_mounts': cbon313bJob_volMnt,
        'env_from':cbon313bJob_env,
        'task_id':'cbon313bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbon313bJob","endTranDate=20241129","requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('1a7191175b5748f0958e36f0bf3bcec5')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbon313bJob,
        Complete
    ]) 

    # authCheck >> cbon313bJob >> Complete
    workflow








