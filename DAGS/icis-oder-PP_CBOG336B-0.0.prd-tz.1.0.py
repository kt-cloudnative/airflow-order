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
                , WORKFLOW_NAME='PP_CBOG336B',WORKFLOW_ID='b531664cdeb94055bbd2960e8257f126', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOG336B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 40, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('b531664cdeb94055bbd2960e8257f126')

    cbog336bJob_vol = []
    cbog336bJob_volMnt = []
    cbog336bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbog336bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbog336bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog336bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog336bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog336bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '5d1c87b39f2d400bb05d67373b39cad8',
        'volumes': cbog336bJob_vol,
        'volume_mounts': cbog336bJob_volMnt,
        'env_from':cbog336bJob_env,
        'task_id':'cbog336bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.7',
        'arguments':["--job.name=cbog336bJob", "endTranDate=202411", "ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('b531664cdeb94055bbd2960e8257f126')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog336bJob,
        Complete
    ]) 

    # authCheck >> cbog336bJob >> Complete
    workflow








