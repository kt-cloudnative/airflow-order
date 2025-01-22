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
                , WORKFLOW_NAME='PP_CBOD107B',WORKFLOW_ID='7313fb0814be4a12a85690aca4d75fee', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOD107B-0.0.prd-tz.2.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 19, 22, 9, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('7313fb0814be4a12a85690aca4d75fee')

    cbod107bJob_vol = []
    cbod107bJob_volMnt = []
    cbod107bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbod107bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbod107bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cbod107bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbod107bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod107bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'cd9d661a607b4b9bb9ab5517e29313cb',
        'volumes': cbod107bJob_vol,
        'volume_mounts': cbod107bJob_volMnt,
        'env_from':cbod107bJob_env,
        'task_id':'cbod107bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=cbod107bJob", "workDate=20240528","requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('7313fb0814be4a12a85690aca4d75fee')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbod107bJob,
        Complete
    ]) 

    # authCheck >> cbod107bJob >> Complete
    workflow








