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
                , WORKFLOW_NAME='IA_CBOT950B',WORKFLOW_ID='07857ad8c927402c85819ef355b97133', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IA_CBOT950B-0.0.prd-tz.6.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 10, 13, 15, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('07857ad8c927402c85819ef355b97133')

    cbot950bJob_vol = []
    cbot950bJob_volMnt = []
    cbot950bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbot950bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbot950bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbot950bJob_env.extend([getICISConfigMap('icis-oder-inetaplca-batch-mng-configmap'), getICISSecret('icis-oder-inetaplca-batch-mng-secret'), getICISConfigMap('icis-oder-inetaplca-batch-configmap'), getICISSecret('icis-oder-inetaplca-batch-secret')])
    cbot950bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot950bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '2caf64bdc85846c89515072c5ebbf384',
        'volumes': cbot950bJob_vol,
        'volume_mounts': cbot950bJob_volMnt,
        'env_from':cbot950bJob_env,
        'task_id':'cbot950bJob',
        'image':'/icis/icis-oder-inetaplca-batch:0.7.1.5',
        'arguments':["--job.name=cbot950bJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('07857ad8c927402c85819ef355b97133')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot950bJob,
        Complete
    ]) 

    # authCheck >> cbot950bJob >> Complete
    workflow








