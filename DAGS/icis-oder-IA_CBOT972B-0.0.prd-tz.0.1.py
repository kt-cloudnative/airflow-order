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
                , WORKFLOW_NAME='IA_CBOT972B',WORKFLOW_ID='645e6a3fbae0472886cd7436d872dae3', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IA_CBOT972B-0.0.prd-tz.0.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 15, 1, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('645e6a3fbae0472886cd7436d872dae3')

    cbot972bJob_vol = []
    cbot972bJob_volMnt = []
    cbot972bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbot972bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbot972bJob_env = [getICISConfigMap('icis-oder-inetaplca-batch-configmap'), getICISConfigMap('icis-oder-inetaplca-batch-configmap2'), getICISSecret('icis-oder-inetaplca-batch-secret')]
    cbot972bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbot972bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot972bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '63ddcb65314545e8b90488f233571150',
        'volumes': cbot972bJob_vol,
        'volume_mounts': cbot972bJob_volMnt,
        'env_from':cbot972bJob_env,
        'task_id':'cbot972bJob',
        'image':'/icis/icis-oder-inetaplca-batch:0.4.1.5',
        'arguments':["--job.name=cbot972bJob", "requestDate="+str(datetime.now()), "procDate=202407"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('645e6a3fbae0472886cd7436d872dae3')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot972bJob,
        Complete
    ]) 

    # authCheck >> cbot972bJob >> Complete
    workflow








