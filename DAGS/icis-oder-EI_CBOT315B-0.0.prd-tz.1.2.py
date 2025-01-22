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
                , WORKFLOW_NAME='EI_CBOT315B',WORKFLOW_ID='592379d98d9140658ff0f05c8bc1c89a', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-EI_CBOT315B-0.0.prd-tz.1.2'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 13, 12, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs':1
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('592379d98d9140658ff0f05c8bc1c89a')

    cbot315bJob_vol = []
    cbot315bJob_volMnt = []
    cbot315bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbot315bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbot315bJob_env = [getICISConfigMap('icis-oder-entprinet-batch-configmap'), getICISConfigMap('icis-oder-entprinet-batch-configmap2'), getICISSecret('icis-oder-entprinet-batch-secret')]
    cbot315bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbot315bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot315bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '4436bb8df9a2468a852ee8072c895351',
        'volumes': cbot315bJob_vol,
        'volume_mounts': cbot315bJob_volMnt,
        'env_from':cbot315bJob_env,
        'task_id':'cbot315bJob',
        'image':'/icis/icis-oder-entprinet-batch:0.4.1.17',
        'arguments':["--job.name=cbot315bJob", "requestDate="+str(datetime.now()), "endTranDate=20240731", "empNo=cbot315b", "empName=cbot315b", "ofcCd=481967", "progName=cbot315b"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cbod445bJob_vol = []
    cbod445bJob_volMnt = []
    cbod445bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbod445bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbod445bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    cbod445bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    cbod445bJob_env = [getICISConfigMap('icis-oder-entprinet-batch-configmap'), getICISConfigMap('icis-oder-entprinet-batch-configmap2'), getICISSecret('icis-oder-entprinet-batch-secret')]
    cbod445bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbod445bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbod445bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '36e9057fe841411f988f650a97b71a3b',
        'volumes': cbod445bJob_vol,
        'volume_mounts': cbod445bJob_volMnt,
        'env_from':cbod445bJob_env,
        'task_id':'cbod445bJob',
        'image':'/icis/icis-oder-entprinet-batch:0.4.1.17',
        'arguments':["--job.name=cbod445bJob", "requestDate="+str(datetime.now()), "procDate=20240731","progName=cbod445b"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('592379d98d9140658ff0f05c8bc1c89a')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot315bJob,
        cbod445bJob,
        Complete
    ]) 

    # authCheck >> cbot315bJob >> cbod445bJob >> Complete
    workflow








