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
                , WORKFLOW_NAME='PP_CSNG933B',WORKFLOW_ID='096cc2af41e2494dbc1593d3f6e136a5', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG933B-0.0.prd-tz.2.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 3, 15, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('096cc2af41e2494dbc1593d3f6e136a5')

    csng933bJob_vol = []
    csng933bJob_volMnt = []
    csng933bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng933bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng933bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng933bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng933bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng933bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '52924c4286794d1db5c2fc2ecdfadd2a',
        'volumes': csng933bJob_vol,
        'volume_mounts': csng933bJob_volMnt,
        'env_from':csng933bJob_env,
        'task_id':'csng933bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.57',
        'arguments':["--job.name=csng933bJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('096cc2af41e2494dbc1593d3f6e136a5')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng933bJob,
        Complete
    ]) 

    # authCheck >> csng933bJob >> Complete
    workflow








