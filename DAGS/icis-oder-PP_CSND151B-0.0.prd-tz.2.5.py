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
                , WORKFLOW_NAME='PP_CSND151B',WORKFLOW_ID='3221c89db8ec4597a99e46b91b0315c3', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSND151B-0.0.prd-tz.2.5'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 29, 12, 9, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('3221c89db8ec4597a99e46b91b0315c3')

    csnd151bJob_vol = []
    csnd151bJob_volMnt = []
    csnd151bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnd151bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnd151bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csnd151bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csnd151bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnd151bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'f0db8cb4940547b1ae50957a84d04471',
        'volumes': csnd151bJob_vol,
        'volume_mounts': csnd151bJob_volMnt,
        'env_from':csnd151bJob_env,
        'task_id':'csnd151bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=csnd151bJob","endTranDate=20240430", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('3221c89db8ec4597a99e46b91b0315c3')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnd151bJob,
        Complete
    ]) 

    # authCheck >> csnd151bJob >> Complete
    workflow








