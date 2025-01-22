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
                , WORKFLOW_NAME='PP_CBOD112B',WORKFLOW_ID='08b98830dcaa46809c0453f04d5f2101', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOD112B-0.0.prd-tz.5.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 10, 25, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('08b98830dcaa46809c0453f04d5f2101')

    cbod112bJob_vol = []
    cbod112bJob_volMnt = []
    cbod112bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbod112bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbod112bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbod112bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbod112bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod112bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '9d712ca040884d2a98219f5a0ca05704',
        'volumes': cbod112bJob_vol,
        'volume_mounts': cbod112bJob_volMnt,
        'env_from':cbod112bJob_env,
        'task_id':'cbod112bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbod112bJob", "endTranDate=20241129", "fromTime=09","toTime=21" ,"requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('08b98830dcaa46809c0453f04d5f2101')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbod112bJob,
        Complete
    ]) 

    # authCheck >> cbod112bJob >> Complete
    workflow








